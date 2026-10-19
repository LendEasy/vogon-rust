"""Regenerate Rust declarations and static metadata from the Python contract.

Development-time tool only. Rust never loads Python contract/configuration code.
The parity harness separately executes the actual Python codecs and models.
"""
import dataclasses as dc
import hashlib
import importlib
import json
from pathlib import Path
import sys
import types
import typing as t

ROOT = Path(__file__).resolve().parents[1]
MONO = ROOT.parent
sys.path[:0] = [str(MONO / 'vogon-contract/src'), str(MONO / 'voice-gateway')]
from vogon_contract import types as contract
from vogon_contract.codec import to_wire
from vogon.gateway.settings import GatewaySettings
from vogon.gateway_service.config import GatewayConfig, TenantProfile


# Keep Rust API roles here so regeneration preserves documentation. Field names,
# defaults and wire tags come from Python. Runtime-nullable overrides below
# account for values emitted by the Python transport despite stricter annotations.
ROLES = {
    'Threshold': 'Named silence or duration rung, measured in elapsed seconds.',
    'FillerPolicy': 'Timing and phrase policy for filler speech while an answer is pending.',
    'RecordingPolicy': 'Per-call recording enablement and channel policy; does not own a recorder.',
    'BargePolicy': 'Caller interruption phrases and semantic-judge policy for floor decisions.',
    'LineDetection': 'Thresholds and timing policy for human versus machine line detection.',
    'CallConfig': 'Application-supplied per-call voice, timer, ambience and interaction policy.',
    'EscalationTarget': 'One telephone destination and its optional display label.',
    'EscalationOrder': 'Requested transfer destinations, whisper, hold and fallback policy.',
    'SpeakCommand': 'Application request for speech with its audio ID and interruption policy.',
    'HeardHorizon': 'Estimated playback prefix heard at a caller onset; not a delivery guarantee.',
    'AbsorbedUtterance': 'Caller text retained during protected or absorbed playback for later context.',
    'UtteranceFinal': 'Definitive caller utterance with confidence, onset and playback context.',
    'UtteranceEager': 'Speculative end-of-turn utterance; later resumption may invalidate the speculation.',
    'UtteranceResumed': 'Notification that caller speech resumed after an eager end-of-turn.',
    'UtteranceAbsorbed': 'Caller utterance recorded without granting an immediate interruption.',
    'DeliveryOutcome': 'Reported speech delivery progress, truncation and estimated heard content.',
    'PlayoutFinished': 'Transport receipt that tracked playback completed, separate from TTS generation.',
    'DrainCleared': 'Playback context captured when buffered outgoing audio is cleared.',
    'EscalationOutcome': 'In-call result of a requested escalation or transfer attempt.',
    'EscalationLegEvent': 'Post-call fact describing one transfer leg and its provider outcome.',
    'EscalationResolved': 'Post-call fact recording the resolved escalation outcome.',
    'LineVerdict': 'Human or machine classification with confidence, source and timing evidence.',
    'SilenceThreshold': 'Event for a named rung reached in the active silence window.',
    'DurationThreshold': 'Event for a named rung reached since the call began.',
    'RecordTransferRequest': 'Request to append transfer facts under a call ID and request ID.',
    'RecordTransferResponse': 'Acknowledgement containing the number of appended transfer facts.',
    'StartCallRequest': 'Turn API request to start or resume an application call session.',
    'StartCallResponse': 'Initial call policy, speech commands and client-facing application events.',
    'ProcessEventsRequest': 'Ordered transport-event batch submitted to the application Turn API.',
    'ClientEvent': 'Application-defined event name and opaque client-facing JSON payload.',
    'ProcessEventsResponse': 'Application decisions: speech, continuation/end, escalation and silence policy.',
    'EndCallRequest': 'Final call reason, remaining events and transport analytics for finalization.',
    'EndCallResponse': 'Empty acknowledgement of application call finalization.',
    'TransportEvent': 'Tagged in-call event union accepted by the Turn API event-processing boundary.',
    'PostCallFact': 'Tagged transfer facts accepted by the separate post-call recording boundary.',
    'GatewaySettings': 'Deployment settings, provider credentials and audio/control defaults. Contains secrets; do not log the full value.',
    'GatewayConfig': 'Service authentication, session-store and bootstrap configuration. Contains a service key; do not log the full value.',
    'TenantProfile': 'Tenant defaults resolved against per-call policy; does not read process environment.',
}


def hint(h):
    if isinstance(h, (str, t.ForwardRef)):
        h = getattr(contract, h if isinstance(h, str) else h.__forward_arg__)
    origin = t.get_origin(h)
    if origin in (t.Union, types.UnionType):
        args = [a for a in t.get_args(h) if a is not type(None)]
        if len(args) == 1: return {'optional': hint(args[0])}
        if set(args) == set(t.get_args(contract.TransportEvent)): return {'union': 'TransportEvent'}
        if set(args) == set(t.get_args(contract.PostCallFact)): return {'union': 'PostCallFact'}
        raise ValueError(h)
    if origin is t.Literal: return {'scalar': 'str'}
    if origin is list: return {'list': hint((t.get_args(h) or (t.Any,))[0])}
    if dc.is_dataclass(h): return {'class': h.__name__}
    return {'scalar': {str: 'str', int: 'int', float: 'float', bool: 'bool', dict: 'dict'}.get(h, 'any')}


def rust_type(h):
    if 'optional' in h: return f'Option<{rust_type(h["optional"])}>'
    if 'list' in h: return f'Vec<{rust_type(h["list"])}>'
    if 'class' in h: return h['class']
    if 'union' in h: return h['union']
    return {'str': 'String', 'int': 'i64', 'float': 'f64', 'bool': 'bool',
            'dict': 'serde_json::Value', 'any': 'serde_json::Value'}[h['scalar']]


def main():
    classes = {name: cls for name, cls in vars(contract).items()
               if isinstance(cls, type) and dc.is_dataclass(cls)}
    schema = {}
    code = ['//! Generated Turn API data types. Roles are maintained in tools/generate_contracts.py.',
            '//! These values describe requests and events; their service implementations are separate.',
            '// Generated by tools/generate_contracts.py; edit Python contract or generator.',
            '// Typed API for well-formed messages; codec::normalize preserves permissive wire behavior.',
            'use serde::{Serialize, Deserialize};', '']
    for name, cls in classes.items():
        fields = {}
        hints = t.get_type_hints(cls, vars(contract))
        kind = getattr(cls, 'kind', None)
        funcs = []
        code += [f'/// {ROLES[name]}', '#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]', f'pub struct {name} {{']
        for f in dc.fields(cls):
            h = hint(hints[f.name])
            # Phone ledger.horizon returns None before the first playout. The
            # Python dataclass/codec accepts that value; Rust must preserve null
            # on the wire rather than reject the first caller utterance.
            if name in ('UtteranceEager', 'UtteranceFinal') and f.name == 'heard':
                h = {'optional': h}
            row = {'hint': h}
            if f.default is not dc.MISSING or f.default_factory is not dc.MISSING:
                default = f.default if f.default is not dc.MISSING else f.default_factory()
                row['default'] = to_wire(default)
                fn = f'default_{name.lower()}_{f.name}'
                literal = json.dumps(json.dumps(row['default'], ensure_ascii=False), ensure_ascii=False)
                funcs.append(f'fn {fn}() -> {rust_type(h)} {{ serde_json::from_str({literal}).expect("generated default") }}')
                code.append(f'    #[serde(default = "{fn}")]')
            field = 'r#type' if f.name == 'type' else f.name
            code.append(f'    pub {field}: {rust_type(h)},')
            fields[f.name] = row
        code += ['}', *funcs, '']
        if kind: code += [f'impl {name} {{ pub const KIND: &\'static str = "{kind}"; }}', '']
        schema[name] = {'fields': fields, 'kind': kind}
    for union, entries in [('TransportEvent', contract.EVENT_TYPES), ('PostCallFact', contract.POST_CALL_FACT_TYPES)]:
        code += [f'/// {ROLES[union]}', '#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]', '#[serde(tag = "kind")]', f'pub enum {union} {{']
        for cls in entries:
            code += [f'    #[serde(rename = "{cls.kind}")]', f'    {cls.__name__}({cls.__name__}),']
        code += ['}', '']
    code += ['/// Exercise the typed boundary after compatibility normalization.',
             'pub fn typed_roundtrip(name: &str, value: &serde_json::Value) -> Result<serde_json::Value, crate::codec::Error> {',
             '    match name {']
    for name in classes:
        code.append(f'        "{name}" => crate::codec::to_wire(name, &crate::codec::from_wire::<{name}>(name, value)?),')
    code += ['        _ => Err(crate::codec::Error::value_error("unknown contract type")),', '    }', '}']
    (ROOT / 'crates/contracts/src/types.rs').write_text('\n'.join(code))
    (ROOT / 'crates/contracts/src/schema.json').write_text(json.dumps(schema, indent=2, ensure_ascii=False) + '\n')
    settings = {}
    model_code = ['//! Generated configuration models; loading and precedence live in the parent module.',
                  '// Generated by tools/generate_contracts.py.', 'use serde::{Serialize, Deserialize};', '']
    for name, cls in [('settings', GatewaySettings), ('gateway', GatewayConfig), ('tenant', TenantProfile)]:
        settings[name] = {n: {'default': f.default, 'hint': hint(f.annotation)} for n, f in cls.model_fields.items()}
        model_code += [f'/// {ROLES[cls.__name__]}', '#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]',f'pub struct {cls.__name__} {{']
        for n,f in cls.model_fields.items(): model_code.append(f'    pub {n}: {rust_type(hint(f.annotation))},')
        model_code += ['}',f'impl {cls.__name__} {{',
                      ('    /// Load typed values from explicit input and defaults; tenant environment inputs are ignored.' if name == 'tenant'
                       else '    /// Load typed values with precedence: explicit input, process environment, dotenv, defaults.'),
                      '    pub fn load(initial: &serde_json::Value, env: &serde_json::Value, dotenv: &serde_json::Value) -> Result<Self, vogon_contracts::codec::Error> {',
                      f'        serde_json::from_value(crate::settings("{name}", initial, env, dotenv)?)',
                      '            .map_err(|e| vogon_contracts::codec::Error::value_error(e.to_string()))',
                      '    }','}', '']
    (ROOT / 'crates/configuration/src/schema.json').write_text(json.dumps(settings, indent=2) + '\n')
    (ROOT / 'crates/configuration/src/models.rs').write_text('\n'.join(model_code))
    provenance = {str(p.relative_to(MONO)): hashlib.sha256(p.read_bytes()).hexdigest() for p in (
        MONO / 'vogon-contract/src/vogon_contract/types.py', MONO / 'voice-gateway/vogon/gateway/settings.py',
        MONO / 'voice-gateway/vogon/gateway_service/config.py')}
    (ROOT / 'tools/generated-sources.json').write_text(json.dumps(provenance, indent=2) + '\n')
    print(f'Generated {len(classes)} contract types and {sum(map(len, settings.values()))} configuration fields')


if __name__ == '__main__': main()
