import argparse
import json
import logging

from app import App

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-c')
    parser.add_argument('--log-level', '-l', default="INFO")

    args = parser.parse_args()
    logging.basicConfig(level=args.log_level)

    app = App()
    with open(args.config, 'r') as f:
        conf = json.load(f)
    app.run(conf)
