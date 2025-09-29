# CommonCrawl NEWS dataset indexer

Creates CDXJ index for the CommonCrawl NEWS dataset (there is official index server).

# Usage

1. Set AWS API key and secret in `boto.cfg` (see example: [example_boto.cfg](example_boto.cfg))
2. Set
   [GNU parallel](https://www.usenix.org/publications/login/february-2011-volume-36-number-1/gnu-parallel-command-line-power-tool)
   `nodefile` (see example: [example_nodefile](example_nodefile))
    - Copy this directory to the same path on all machines
3. Set parameters as environment variables:

    - PYTHON (default: python3)
    - OUTPUT_DIR (default: $(PWD)/output)
    - BOTO_CFG (default: $(PWD)/boto.cfg)
    - NO_OF_THREADS (default: 80)
    - NICEVALUE (default: 10)

4. Set languages to collect in [languages_to_collect.txt](languages_to_collect.txt). The format
   is `"[LANGUAGE NAME AS IN LINGUA]":` (because it is grepped from a JSONL for speed concerns)

Run `make` to execute the whole process or consult with the Makefile for the individual steps

# License

This code is licensed under the GPL 3.0 license.

# Acknowledgements

The authors acknowledge the support of the National Laboratory for Digital
Heritage. Project no. 2022-2.1.1-NL-2022-00009 has been implemented with the
support provided by the Ministry of Culture and Innovation of Hungary from the
National Research, Development and Innovation Fund, financed under the
2022-2.1.1-NL funding scheme.
