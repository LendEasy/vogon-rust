"""Run the real Python Turn app with a primed demo fixture and read-only commit tap.

This launcher owns an isolated process/database/storage tree. It does not replace
reasoning, perception, tools, HTTP handlers or the checkpoint implementation.
"""
import argparse
import asyncio
import copy
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import sys

MONO = Path(__file__).resolve().parents[3]


async def run(fixture):
    from app.services import checkpoint_store
    from app.services.config_client import set_cached_config
    from app.turns.main import app
    from app.turns.http_service import _calls
    from app.database import SessionLocal
    from app.models.call_record import CallRecord
    from app.services.redis_client import get_redis, close_redis
    from evals.runner.fixtures import build_config
    import uvicorn

    out = Path(fixture['out'])
    config = build_config()
    config['workflow_definition'] = json.loads(
        (MONO / 'data-plane/app/workflows/definitions/collections_ladders.json').read_text()
    )
    config['assistant']['persona_voice'] = 'aura-2-orion-en'
    config['assistant']['ambience'] = {'enabled': False}
    config['assistant']['enable_empathy_fillers'] = False
    await set_cached_config(fixture['session_token'], config)
    (out / 'demo-config.json').write_text(json.dumps(config, indent=2, default=str))
    commits = []
    original = checkpoint_store.commit

    async def capture(call_id, state, expected_version, *args, **kwargs):
        version = await original(call_id, state, expected_version, *args, **kwargs)
        if call_id == fixture['call_id']:
            row = copy.deepcopy(state)
            commits.append(row)
            (out / 'checkpoints' / f'{len(commits):03d}.json').write_text(
                json.dumps(row, indent=2, default=str)
            )
        return version

    checkpoint_store.commit = capture
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def observed_lifespan(application):
        """Seed normal intake state, then observe cleanup before Uvicorn re-raises SIGTERM."""
        async with original_lifespan(application):
            with SessionLocal() as db:
                db.add(CallRecord(id=fixture['call_id'], session_token=fixture['session_token'],
                                  owner_id='e2e-ladders', workflow_id='collections_ladders',
                                  state='connected', mode='twilio'))
                db.commit()
            try:
                yield
            finally:
                cid = fixture['call_id']
                remaining = await checkpoint_store.load(cid)
                redis = await get_redis()
                jobs = await redis.xrange('call_end_jobs') if redis else []
                with SessionLocal() as db:
                    record = db.get(CallRecord, cid)
                    record_state = record.state if record else None
                (out / 'python-cleanup.json').write_text(json.dumps({
                    'commits': len(commits), 'registry_remaining': cid in _calls,
                    'checkpoint_remaining': remaining is not None,
                    'record_state': record_state,
                    'post_call_jobs': sum(fields.get('call_id') == cid for _, fields in jobs),
                }, indent=2))
                await close_redis()

    app.router.lifespan_context = observed_lifespan
    server = uvicorn.Server(uvicorn.Config(
        app, host='127.0.0.1', port=fixture['turn_port'], log_level='info'
    ))
    await server.serve()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', required=True)
    args = parser.parse_args()
    fixture = json.loads(Path(args.fixture).read_text())
    for directory in ['data-plane', 'voice-gateway', 'vogon-contract/src']:
        sys.path.insert(0, str(MONO / directory))
    from dotenv import load_dotenv
    load_dotenv(fixture['env_path'], override=False)
    os.chdir(MONO / 'data-plane')
    asyncio.run(run(fixture))


if __name__ == '__main__':
    main()
