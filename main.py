import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, default="~/.juqueue")
    parser.parse_args()

