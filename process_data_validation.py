"""
Process counts for all PG data.

Written by
M. Gerlach and F. Font-Clos

"""
import os
from os.path import join
import argparse
import glob
import ast
import pandas as pd
import traceback
import nltk

from src.pipeline import process_book
from src.utils import get_langs_dict

# Ensure NLTK resources are downloaded
try:
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt_tab')

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        "Processing raw texts from Project Gutenberg:"
        " i) removing headers,ii) tokenizing, and iii) counting words.")
    parser.add_argument(
        "-r", "--raw",
        help="Path to the raw-folder",
        default='data_validation/raw/',
        type=str)
    parser.add_argument(
        "-ote", "--output_text",
        help="Path to text-output (text_dir)",
        default='data_validation/text/',
        type=str)
    parser.add_argument(
        "-oto", "--output_tokens",
        help="Path to tokens-output (tokens_dir)",
        default='data_validation/tokens/',
        type=str)
    parser.add_argument(
        "-oco", "--output_counts",
        help="Path to counts-output (counts_dir)",
        default='data_validation/counts/',
        type=str)
    parser.add_argument(
        "-p", "--pattern",
        help="Patttern to specify a subset of books",
        default='*',
        type=str)
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Quiet mode, do not print info, warnings, etc"
    )
    parser.add_argument(
        "-l", "--log_file",
        help="Path to log file",
        default=".log",
        type=str)

    args = parser.parse_args()

    if os.path.isdir(args.output_text) is False:
        raise ValueError(f"Text output directory '{args.output_text}' does not exist.")
    if os.path.isdir(args.output_tokens) is False:
        raise ValueError(f"Tokens output directory '{args.output_tokens}' does not exist.")
    if os.path.isdir(args.output_counts) is False:
        raise ValueError(f"Counts output directory '{args.output_counts}' does not exist.")

    metadata = pd.read_csv("metadata/metadata.csv").set_index("id")
    langs_dict = get_langs_dict()

    pbooks = 0
    for filename in glob.glob(join(args.raw, 'PG*_raw.txt')):
        try:
            file_basename = os.path.basename(filename)
            PG_id = file_basename.split("_")[0]

            if not PG_id.startswith("PG") or not PG_id[2:].isdigit():
                if not args.quiet:
                    print(f"# WARNING: Invalid ID '{PG_id}'. Skipping.")
                continue
            
            pg_num = int(PG_id[2:])
            if pg_num < 10135 or pg_num >= 10165:
                continue

            if PG_id not in metadata.index:
                if not args.quiet:
                    print(f"# WARNING: Metadata missing for {PG_id}. Skipping.")
                continue

            # Strict language filtering - MODIFIED SECTION
            lang_list = ast.literal_eval(metadata.loc[PG_id, "language"])
            if lang_list != ['en']:  # Changed condition
                if not args.quiet:
                    print(f"# WARNING: Non-English/multilingual book {PG_id} - Skipping.")
                continue

            lang_id = lang_list[0]
            language = langs_dict.get(lang_id, "english")

            process_book(
                path_to_raw_file=filename,
                text_dir=args.output_text,
                tokens_dir=args.output_tokens,
                counts_dir=args.output_counts,
                language=language,
                log_file=args.log_file
            )
            pbooks += 1
            if not args.quiet:
                print(f"Processed {pbooks} books...", end="\r")
                
        except UnicodeDecodeError:
            if not args.quiet:
                print(f"# WARNING: Encoding error in '{file_basename}'")
        except KeyError as e:
            if not args.quiet:
                print(f"# WARNING: Metadata field missing for {PG_id} - {str(e)}")
        except Exception as e:
            if not args.quiet:
                print(f"# ERROR: Failed to process '{file_basename}' - {str(e)}")
                traceback.print_exc()
