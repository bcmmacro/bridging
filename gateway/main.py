import argparse
import json
import logging

from app import App

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-c')

    args = parser.parse_args()

    app = App()
    with open(args.config, 'r') as f:
        conf = json.load(f)
    app.run(conf)
