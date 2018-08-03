import argparse
import ConfigParser
from scalableor.sampler import Sampler

cfg = ConfigParser.ConfigParser()
cfg.read("config.ini")

parser = argparse.ArgumentParser(description="Start sampler")

parser.add_argument("input", help="set path to input file", type=str)

parser.add_argument("--max_size", default=cfg.get("sampler", "max-size"),
                    help="set maximal size of sample in MiB (default: %(default)s)", type=float)

args = parser.parse_args()

# Set sampler configuration
Sampler.max_size = args.max_size

# Create sample
sample = Sampler(args.input)
sample.save()
