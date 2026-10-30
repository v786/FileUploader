"""Chaos: a random fault profile per run. The seed is printed so a failure
replays exactly: CHAOS_SEED=<seed> make chaos"""
import os
import random

from fileuploader import db
from tests.conftest import spec


def test_random_fault_profile(env):
    seed = int(os.environ.get("CHAOS_SEED") or random.SystemRandom().randrange(1 << 30))
    print(f"\nCHAOS_SEED={seed}")
    rng = random.Random(seed)
    random.seed(seed)
    for v in ("scanner", "extractor", "thumbnailer"):
        profile = {"error_rate": round(rng.uniform(0, 0.35), 2), "error_status": rng.choice([500, 502, 503, 429])}
        if rng.random() < 0.3:
            profile["rate_limit_rps"] = rng.randint(2, 10)
        env.faults(v, profile)

    names = [(f"c{i}.{rng.choice(['txt', 'pdf', 'png', 'csv'])}", os.urandom(rng.randint(0, 3000))) for i in range(12)]
    files = env.create_batch([spec(n, d) for n, d in names])["files"]
    for f, (_, d) in zip(files, names):
        assert env.upload(f, d).status_code == 200

    env.drain(rounds=400, step=60)

    with db.read() as conn:
        statuses = [r[0] for r in conn.execute("SELECT status FROM files")]
        # Exactly one cached result per (file, stage, vendor), never more.
        dup = conn.execute("SELECT COUNT(*) - COUNT(DISTINCT idempotency_key) FROM vendor_calls").fetchone()[0]
        no_reason = conn.execute("SELECT COUNT(*) FROM files WHERE status='failed' AND error_code IS NULL").fetchone()[0]
    assert set(statuses) <= {"completed", "failed"}, f"seed {seed}: {statuses}"
    assert dup == 0 and no_reason == 0
