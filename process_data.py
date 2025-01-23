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
import traceback  # Added for detailed error logging

from src.pipeline import process_book
from src.utils import get_langs_dict


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        "Processing raw texts from Project Gutenberg:"
        " i) removing headers,ii) tokenizing, and iii) counting words.")
    # raw folder
    parser.add_argument(
        "-r", "--raw",
        help="Path to the raw-folder",
        default='data/raw/',
        type=str)
    # text folder
    parser.add_argument(
        "-ote", "--output_text",
        help="Path to text-output (text_dir)",
        default='data/text/',
        type=str)
    # tokens folder
    parser.add_argument(
        "-oto", "--output_tokens",
        help="Path to tokens-output (tokens_dir)",
        default='data/tokens/',
        type=str)
    # counts folder
    parser.add_argument(
        "-oco", "--output_counts",
        help="Path to counts-output (counts_dir)",
        default='data/counts/',
        type=str)
    # pattern to specify subset of books
    parser.add_argument(
        "-p", "--pattern",
        help="Patttern to specify a subset of books",
        default='*',
        type=str)

    # quiet argument, to supress info
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Quiet mode, do not print info, warnings, etc"
    )

    # log file
    parser.add_argument(
        "-l", "--log_file",
        help="Path to log file",
        default=".log",
        type=str)

    # add arguments to parser
    args = parser.parse_args()

    # check whether the out-put directories exist
    if os.path.isdir(args.output_text) is False:
        raise ValueError("The directory for output of texts '%s' "
                         "does not exist" % (args.output_text))
    if os.path.isdir(args.output_tokens) is False:
        raise ValueError("The directory for output of tokens '%s' "
                         "does not exist" % (args.output_tokens))
    if os.path.isdir(args.output_counts) is False:
        raise ValueError("The directory for output of counts '%s' "
                         "does not exist" % (args.output_counts))

    # load metadata
    metadata = pd.read_csv("metadata/metadata.csv").set_index("id")

    # load languages dict
    langs_dict = get_langs_dict()

    # loop over all books in the raw-folder
    pbooks = 0
    for filename in glob.glob(join(args.raw, 'PG*_raw.txt')):
        try:
            # Extract NUMERIC ID from filename (e.g., "PG123_raw.txt" -> "123")
            file_basename = os.path.basename(filename)
            PG_id_numeric = file_basename.split("_")[0][2:]  # Remove "PG" prefix
            
            # Skip invalid or out-of-range IDs
            if not PG_id_numeric.isdigit() or int(PG_id_numeric) > 100:
                continue

            # Check if metadata exists for this book
            if PG_id_numeric not in metadata.index:
                if not args.quiet:
                    print(f"# WARNING: Metadata missing for PG{PG_id_numeric}. Skipping.")
                continue

            # Get language from metadata
            lang_list = ast.literal_eval(metadata.loc[PG_id_numeric, "language"])
            lang_id = lang_list[0]  # Use first language code
            language = langs_dict.get(lang_id, "english")  # Fallback to English

            # Process the book
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
                print(f"# WARNING: Metadata field missing for PG{PG_id_numeric} - {str(e)}")
        except Exception as e:
            if not args.quiet:
                print(f"# ERROR: Failed to process '{file_basename}' - {str(e)}")
                traceback.print_exc()  # Debugging
