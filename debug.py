import argparse
import importlib

import questionary
from dask.distributed import Client
from questionary import Choice

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    name = parser.parse_args().name

    mod = importlib.import_module(f"experiments.{name}")

    print("Experiment available as experiment variable.")
    experiment = mod.Experiment()

    runs = experiment.runs

    run = questionary.select(f"Choose a run ({len(runs)} found):", [Choice(run.run_id, run) for run in runs]).ask()

    client = Client()

    for k, v in run.env.items():
        print(f"export {k}={v}")
    print(f"mkdir -p {run.path}")
    print(f"cd {run.path}")
    print(f"{experiment.executor.create_script(run)}")
