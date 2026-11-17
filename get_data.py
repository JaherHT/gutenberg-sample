"""
Download a fraction of the Project Gutenberg sample and build metadata.csv.

The sample is the books PG10000 to PG10134. Books are downloaded over plain
HTTP, so no rsync or other system tool is needed.

Written by
M. Gerlach & F. Font-Clos

"""
from src.metadataparser import make_df_metadata
from src.bookshelves import parse_bookshelves

import argparse
import os
import pickle
import random
import time
import urllib.error
import urllib.request

SAMPLE_IDS = range(10000, 10135)
DEFAULT_BASE_URL = "https://www.gutenberg.org/cache/epub"


def select_sample_ids(fraction=1.0, seed=0, ids=SAMPLE_IDS):
    """
    Pick a reproducible random subset of the sample ids.

    The same fraction and seed always return the same sorted list.
    """
    if not 0 < fraction <= 1:
        raise ValueError("fraction must be greater than 0 and at most 1.")
    ids = list(ids)
    n = max(1, round(len(ids) * fraction))
    return sorted(random.Random(seed).sample(ids, n))


def download_book(book_id, raw_dir, base_url=DEFAULT_BASE_URL,
                  overwrite=False, timeout=60):
    """
    Download one book to raw_dir/PG<id>_raw.txt.

    Returns 'downloaded', 'skipped' (file already there) or 'missing' (404).
    Other network errors are raised.
    """
    target = os.path.join(raw_dir, "PG%d_raw.txt" % book_id)
    if os.path.isfile(target) and not overwrite:
        return "skipped"

    url = "%s/%d/pg%d.txt" % (base_url.rstrip("/"), book_id, book_id)
    request = urllib.request.Request(
        url, headers={"User-Agent": "gutenberg-sample-fetch/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data = response.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return "missing"
        raise

    # write to a temporary name first, so an interrupted download
    # is never mistaken for a complete file on the next run
    partial = target + ".part"
    with open(partial, "wb") as f:
        f.write(data)
    os.replace(partial, target)
    return "downloaded"


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description="Download a fraction of the Project Gutenberg sample "
                    "(PG10000-PG10134) and build the metadata csv.")
    parser.add_argument(
        "-f", "--fraction",
        help="Fraction of the sample to download, above 0 and at most 1. "
             "Default 1.0 downloads all 135 books.",
        default=1.0,
        type=float)
    parser.add_argument(
        "-s", "--seed",
        help="Seed for choosing the books. The same seed gives the same books.",
        default=0,
        type=int)
    parser.add_argument(
        "-r", "--raw",
        help="Path to the raw folder.",
        default='data/raw/',
        type=str)
    parser.add_argument(
        "-M", "--metadata",
        help="Path to the metadata folder.",
        default='metadata/',
        type=str)
    parser.add_argument(
        "-k", "--keep_rdf",
        action="store_false",
        help="If there is an RDF file in metadata dir, do not overwrite it.")
    parser.add_argument(
        "-owr", "--overwrite_raw",
        action="store_true",
        help="Download again books already in raw.")
    parser.add_argument(
        "-u", "--base_url",
        help="Base URL of the books, e.g. a local mirror. "
             "Book n is fetched from <base_url>/<n>/pg<n>.txt",
        default=DEFAULT_BASE_URL,
        type=str)
    parser.add_argument(
        "-w", "--wait",
        help="Seconds to wait between downloads.",
        default=1.0,
        type=float)
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Quiet mode, do not print info, warnings, etc")

    args = parser.parse_args()
    if not 0 < args.fraction <= 1:
        parser.error("--fraction must be above 0 and at most 1.")

    os.makedirs(args.raw, exist_ok=True)
    os.makedirs(args.metadata, exist_ok=True)

    ids = select_sample_ids(args.fraction, args.seed)
    if not args.quiet:
        print(f"Selected {len(ids)} of {len(SAMPLE_IDS)} sample books "
              f"(fraction {args.fraction}, seed {args.seed}).")

    counts = {"downloaded": 0, "skipped": 0, "missing": 0, "failed": 0}
    for book_id in ids:
        try:
            status = download_book(
                book_id, args.raw, base_url=args.base_url,
                overwrite=args.overwrite_raw)
        except (urllib.error.URLError, OSError) as e:
            status = "failed"
            if not args.quiet:
                print(f"# WARNING: PG{book_id} failed - {e}")
        if status == "missing" and not args.quiet:
            print(f"# WARNING: PG{book_id} not found on the server.")
        counts[status] += 1
        if status == "downloaded" and args.wait > 0:
            time.sleep(args.wait)

    if not args.quiet:
        print(f"Downloaded {counts['downloaded']}, "
              f"already present {counts['skipped']}, "
              f"not found {counts['missing']}, "
              f"failed {counts['failed']}.")

    make_df_metadata(
        path_xml=os.path.join(args.metadata, 'rdf-files.tar.bz2'),
        path_out=os.path.join(args.metadata, 'metadata.csv'),
        update=args.keep_rdf
        )

    BS_dict, BS_num_to_category_str_dict = parse_bookshelves()
    with open("metadata/bookshelves_ebooks_dict.pkl", 'wb') as fp:
        pickle.dump(BS_dict, fp)
    with open("metadata/bookshelves_categories_dict.pkl", 'wb') as fp:
        pickle.dump(BS_num_to_category_str_dict, fp)
