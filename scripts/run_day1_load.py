from pathlib import Path
import sys
import logging

# Make 'src/' importable when running as a script
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    paths = make_paths(ROOT)

    # read raw csvs
    orders = read_orders_csv(paths.raw_orders)
    users = read_users_csv(paths.raw_users)

    # enforce schema on orders (numbers + types)
    orders = enforce_schema(orders)

    # write parquet outputs
    write_parquet(orders, paths.orders_parquet)
    write_parquet(users, paths.users_parquet)

    log.info("Wrote: %s", paths.orders_parquet)
    log.info("Wrote: %s", paths.users_parquet)


if __name__ == "__main__":
    main()