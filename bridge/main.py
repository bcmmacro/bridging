import logging

import argparse

from app import App

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-c')

    args = parser.parse_args()

    app = App()
    app.run(args.config)
