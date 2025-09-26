#!/bin/bash
git pull
# Run with default output directory (current directory)
python ./downloader.py -o .

# To run with a custom output directory, uncomment the line below:
# python ./downloader.py -o /path/to/output/directory