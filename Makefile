PY ?= .venv/bin/python
PORT ?= 8000

.PHONY: install run api worker test chaos fixtures demo clean web-install web-dev web-test web-build

install:
	python3 -m venv .venv
	$(PY) -m pip install -r requirements.txt

run:            ## API + workers + relay + sweeper in one process
	$(PY) -m fileuploader all --port $(PORT)

api:            ## API only (scale independently)
	$(PY) -m fileuploader api --port $(PORT)

worker:         ## workers + outbox relay + sweeper only
	$(PY) -m fileuploader worker

test:
	$(PY) -m pytest -q

chaos:          ## random fault profile per run; replay with CHAOS_SEED=<seed>
	for i in 1 2 3 4 5; do $(PY) -m pytest -q -s tests/test_chaos.py || exit 1; done

fixtures:
	$(PY) scripts/make_fixtures.py

demo: fixtures  ## needs `make run` in another terminal
	$(PY) scripts/demo_client.py --api http://127.0.0.1:$(PORT) fixtures/*

clean:
	rm -rf data .pytest_cache

web-install:    ## React frontend deps (needs Node 18+)
	cd web && npm install

web-dev:        ## demo app on :5173, proxies the API on :$(PORT) (run `make run` too)
	cd web && API_URL=http://127.0.0.1:$(PORT) npm run dev

web-test:
	cd web && npm test && npm run typecheck

web-build:
	cd web && npm run build
