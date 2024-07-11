PYTHON?=python3
OUTPUT_DIR?=$(PWD)/output
BOTO_CFG?=$(PWD)/boto.cfg
NO_OF_THREADS?=80
NICEVALUE?=10

all: venv toc index filter_language

venv:
	$(PYTHON) -m venv venv
	./venv/bin/pip install --upgrade 'pip>=21.0'  # Magika needs newer pip!
	./venv/bin/pip install -r requirements.txt

clean:
	rm -rf venv toc.txt

toc:
	./venv/bin/python get_toc.py > toc.txt

index:
	# About 277 141 386 lines in total
	# About 6700 minutes in total
	rm -rf $(OUTPUT_DIR)
	cat toc.txt | parallel --will-cite --pipe --sshloginfile nodefile --sshdelay 1 \
	-N $(shell expr $$(cat toc.txt | wc -l) / $(NO_OF_THREADS) + 1) \
	$(PWD)/venv/bin/python $(PWD)/create_index.py -n $(NICEVALUE) -k - -o $(OUTPUT_DIR) -c $(BOTO_CFG)

filter_language:
	# About 25 minutes
	rm -rf tmp && mkdir tmp
	ls $(OUTPUT_DIR) | parallel --will-cite -I {} zgrep -Ff languages_to_collect.txt {} | \
    LC_ALL="C.UTF-8" sort --parallel="$$(nproc)" -T./tmp | pigz > CC_NEWS_INDEX_LANG_FILTERED_SORTED.cdxj.gz
	pigz -cd CC_NEWS_INDEX_LANG_FILTERED_SORTED.cdxj.gz | python3 sum_net_words_by_language.py | \
    pigz > wc_w_for_langs.txt.gz
