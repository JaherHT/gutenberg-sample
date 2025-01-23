"""
Project Gutenberg parsing with python 3.

Written by
M. Gerlach & F. Font-Clos

"""
from src.utils import populate_raw_from_mirror, list_duplicates_in_mirror
from src.metadataparser import make_df_metadata
from src.bookshelves import get_bookshelves
from src.bookshelves import parse_bookshelves

import argparse
import os
import subprocess
import pickle

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        "Update local PG repository.\n\n"
        "This script will download all books currently not in your\n"
        "local copy of PG and get the latest version of the metadata.\n"
        )
    # mirror dir
    parser.add_argument(
        "-m", "--mirror",
        help="Path to the mirror folder that will be updated via rsync.",
        default='data/.mirror/',
        type=str)

    # raw dir
    parser.add_argument(
        "-r", "--raw",
        help="Path to the raw folder.",
        default='data/raw/',
        type=str)

    # metadata dir
    parser.add_argument(
        "-M", "--metadata",
        help="Path to the metadata folder.",
        default='metadata/',
        type=str)

    # Remove the pattern argument since we're hardcoding the selection
    # update argument
    parser.add_argument(
        "-k", "--keep_rdf",
        action="store_false",
        help="If there is an RDF file in metadata dir, do not overwrite it.")

    # update argument
    parser.add_argument(
        "-owr", "--overwrite_raw",
        action="store_true",
        help="Overwrite files in raw.")

    # quiet argument, to supress info
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Quiet mode, do not print info, warnings, etc"
        )

    # create the parser
    args = parser.parse_args()

    # check that all dirs exist
    if not os.path.isdir(args.mirror):
        raise ValueError("The specified mirror directory does not exist.")
    if not os.path.isdir(args.raw):
        raise ValueError("The specified raw directory does not exist.")
    if not os.path.isdir(args.metadata):
        raise ValueError("The specified metadata directory does not exist.")

    # Update the .mirror directory via rsync
    # --------------------------------------
    # We sync the 'mirror_dir' with PG's site via rsync
    # The pattern now includes only the first 100 book IDs

    # pass the -v flag to rsync if not in quiet mode
    if args.quiet:
        vstring = ""
    else:
        vstring = "v"

    # Generate include patterns for book IDs 1 to 100
    includes = []
    for book_id in range(1, 101):
        # Pattern matches files starting with pg{id} followed by [.-] and other suffixes
        pattern = f"pg{book_id}[.-][t0][x.]t[x.]*[t8]"
        includes.extend(["--include", pattern])

    sp_args = ["rsync", "-am%s" % vstring,
               "--include", "*/"] + includes + [
               "--exclude", "*",
               "aleph.gutenberg.org::gutenberg", args.mirror
               ]
    subprocess.call(sp_args)

    # Get rid of duplicates
    # ---------------------
    dups_list = list_duplicates_in_mirror(mirror_dir=args.mirror)

    # Populate raw from mirror
    # ------------------------
    populate_raw_from_mirror(
        mirror_dir=args.mirror,
        raw_dir=args.raw,
        overwrite=args.overwrite_raw,
        dups_list=dups_list,
        quiet=args.quiet
        )

    # Update metadata
    # ---------------
    make_df_metadata(
        path_xml=os.path.join(args.metadata, 'rdf-files.tar.bz2'),
        path_out=os.path.join(args.metadata, 'metadata.csv'),
        update=args.keep_rdf
        )

    # Bookshelves
    # -----------
    BS_dict, BS_num_to_category_str_dict = parse_bookshelves()
    with open("metadata/bookshelves_ebooks_dict.pkl", 'wb') as fp:
        pickle.dump(BS_dict, fp)
    with open("metadata/bookshelves_categories_dict.pkl", 'wb') as fp:
        pickle.dump(BS_num_to_category_str_dict, fp)
