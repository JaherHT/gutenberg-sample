# Standardized Project Gutenberg Corpus
Easily generate a local, up-to-date copy of the Standardized Project Gutenberg Corpus (SPGC).

The Standardized Project Gutenberg Corpus was presented in 

[A standardized Project Gutenberg corpus for statistical analysis of natural language and quantitative linguistics](https://arxiv.org/abs/1812.08092)  
M. Gerlach, F. Font-Clos, arXiv:1812.08092, Dec 2018

acompanied by a 'frozen' version of the corpus (SPGC-2018-07-18) as a Zenodo dataset: 

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.2422560.svg)](https://doi.org/10.5281/zenodo.2422560)

SPGC-2018-07-18 contains the `tokens/` and `counts/` files of all books that were part of Project Gutenbergh (PG) as of Jul 18, 2018, matching exactly those used in the paper. Since then, a few more thousands books have been added to PG, so if you want to exactly reproduce the results of the paper, then you should use SPGC-2018-07-18.

For **most other use cases**, however, you probably want the latest, most recent version of the corpus, in which case you should use this repository to **generate the corpus locally** on your computer. In particular, you will need to generate the corpus locally if you need to work with the original full text files in `raw/` and `text/`, since these are not included in the SPGC-2018-07-18 Zenodo dataset.


## Installation
:warning: **Python 2.x is not supported** Please make sure your system runs Python 3.x. (https://pythonclock.org/).  

Clone this repository

```bash
git clone https://github.com/pgcorpus/gutenberg.git
```
enter the newly created `gutenberg` directory

```bash
cd gutenberg
```

To install any missing dependencies, just run

```bash
pip install -r requirements.txt
```

## Getting the data
This repository is the sample: books PG10000 to PG10134 (135 books). Books are downloaded over plain HTTP, so no rsync is needed.

To get all 135 books, run
```bash
python get_data.py
```

To get only a fraction, use `--fraction`. The books are picked at random, and `--seed` makes the pick repeatable:
```bash
python get_data.py --fraction 0.1
```

This writes the books to `data/raw/` and a csv with metadata (e.g. author, title, year, ...) to `metadata/`.

- Books already in `data/raw/` are skipped, so you can run it again to add more.
- Books that the server does not have are reported and skipped.
- The program waits 1 second between downloads by default. Change this with `--wait`, or point `--base_url` at a mirror.


## Processing the data
To process all the data in the `raw/` directory, run
```bash
python process_data.py
```
This will fill in the `text/`, `tokens/` and `counts/` folders.



