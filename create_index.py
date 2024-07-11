import os
import re
import sys
import json
import gzip
import warnings
from time import sleep
from pathlib import Path
from urllib.parse import urlparse
from argparse import ArgumentParser
from configparser import ConfigParser

from surt import surt
from boto3 import client
# from magika import Magika
from warcio import ArchiveIterator
from warcio.timeutils import iso_date_to_timestamp
from magic import from_buffer as magic_from_buffer
from lingua import LanguageDetectorBuilder  # , Language
from botocore.exceptions import ReadTimeoutError, IncompleteReadError, ResponseStreamingError, ClientError
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning, XMLParsedAsHTMLWarning
warnings.filterwarnings('error')

# Init detectors
# m = Magika()
detector = LanguageDetectorBuilder.from_all_languages().build()
# List all languages:
# print(Language.all())


class AIMD:
    """https://en.wikipedia.org/wiki/Additive_increase/multiplicative_decrease"""

    def __init__(self, initial_value, increase_value, decrease_value):
        self._min_value = initial_value
        self._value = initial_value
        self._increase_value = increase_value
        self._decrease_value = decrease_value

    def __call__(self):
        return self._value

    def increase(self):
        self._value += self._increase_value

    def decrease(self):
        self._value //= self._decrease_value
        self._value = max(self._value, self._min_value)

    def __repr__(self):
        return f'AIMD({self._value}, {self._increase_value}, {self._decrease_value})'


# Parse args
def parse_args():
    def abs_or_rel_file(val):
        path_val = Path(val)
        if not path_val.is_absolute():
            path_val = Path('__file__').absolute().parent / val

        if not path_val.exists() or not path_val.is_file():
            raise ValueError(f'Path (val) file not exits!')

        return path_val

    def abs_or_rel_dir(val):
        path_val = Path(val)
        if not path_val.is_absolute():
            path_val = Path('__file__').absolute().parent / val

        if path_val.exists() and not path_val.is_dir():
            raise ValueError(f'Path (val) is not a directory!')

        return path_val

    def non_empty_str(val):
        if len(val) == 0:
            raise ValueError(f'Must not be empty!')
        return val

    def positive_int(val):
        try:
            int_val = int(val)
        except ValueError:
            int_val = -1

        if int_val <= -1:
            raise ValueError(f'Must be positive int ({val})!')
        return int_val

    parser = ArgumentParser()
    parser.add_argument('-c', '--credentials', type=abs_or_rel_file,
                        default=Path('__file__').absolute().parent / 'boto.cfg',
                        help='Credentials INI (default section, aws_access_key_id and aws_access_key_id, '
                             'default: boto.cfg)')
    parser.add_argument('-o', '--out_dir', type=abs_or_rel_dir,
                        default=Path('__file__').absolute().parent / 'output',
                        help='Output directory (default: output)')
    parser.add_argument('-k', '--key_name', type=non_empty_str, default='-',
                        help='Key name to process (default: - for STDIN)')
    parser.add_argument('--offset', type=positive_int,
                        help='Offset to use')
    parser.add_argument('--length', type=positive_int,
                        help='Length to use')
    parser.add_argument('-n', '--nice', type=positive_int, default=10,
                        help='Nice value to use (only 0 or greater is allowed)')
    args = parser.parse_args()

    # Read config
    config = ConfigParser()
    config.read(args.credentials)
    aws_access_key_id = config.get('default', 'aws_access_key_id', fallback='')
    aws_secret_access_key = config.get('default', 'aws_secret_access_key', fallback='')

    if len(aws_access_key_id) == 0 or len(aws_secret_access_key) == 0:
        print(f'The variables AWS_ACCESS_KEY_ID ({aws_access_key_id}) and '
              f'AWS_SECRET_ACCESS_KEY (aws_secret_access_key) are not set or empty '
              f'(checked in {args.credentials})!', file=sys.stderr, flush=True)
        exit(1)

    return aws_access_key_id, aws_secret_access_key, args.key_name, args.offset, args.length, args.out_dir, args.nice


def main():
    aws_access_key_id, aws_secret_access_key, key_name, offset, length, output_dir, nice = parse_args()

    # Set nice value
    os.nice(nice)

    # Login to S3
    s3 = client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # Download keys read from STDIN or single key from parameter
    if key_name == '-':
        from_stdin(s3, output_dir)
    else:
        single_object(s3, key_name, offset, length)


def single_object(s3, key_name, offset, length):
    try:
        s3_obj = s3_get_object(s3, key_name, offset, length)

        # Write CDXJ index lines (unsorted)
        sys.stdout.writelines(process_archive(key_name, s3_obj['Body']))

    except (ClientError, ReadTimeoutError, IncompleteReadError, ResponseStreamingError) as e:
        # If IncompleteReadError: https://github.com/boto/boto3/issues/3781#issuecomment-1717913787
        print(f'ClientError/ReadTimeoutError/IncompleteReadError/ResponseStreamingError '
              f'(probably rate limited)', e, key_name, file=sys.stderr, flush=True)
        exit(1)


def from_stdin(s3, output_dir):
    # Set amid
    aimd = AIMD(1000, 100, 2)  # TODO fine-tune this ;)
    max_retries = 4

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Read the key names from stdin
    for key_name in sys.stdin:
        key_name = key_name.rstrip('\n')

        # Determine output filename
        out_filename = Path(key_name).stem[:-5]  # Remove .warc ending

        retry = 0
        while retry < max_retries:  # Retry max_retries times, before permanently fails
            try:
                with gzip.open(output_dir / f'{out_filename}.cdxj.gz', 'wt', encoding='UTF-8') as fh:
                    s3_obj = s3_get_object(s3, key_name)

                    # Write CDXJ index lines (sorted)
                    fh.writelines(sorted(process_archive(key_name, s3_obj['Body'])))
                    aimd.decrease()
                    break
            except (ClientError, ReadTimeoutError, IncompleteReadError, ResponseStreamingError) as e:
                aimd.increase()
                # If IncompleteReadError: https://github.com/boto/boto3/issues/3781#issuecomment-1717913787
                print(f'ClientError/ReadTimeoutError/IncompleteReadError/ResponseStreamingError '
                      f'(probably rate limited, new AIMD value: {aimd()}, retry: {retry})',
                      e, key_name, file=sys.stderr, flush=True)
                sleep(aimd())
                retry += 1
        else:
            print('ERROR: Got None for s3 obj', key_name, file=sys.stderr, flush=True)


def s3_get_object(s3, key_name, offset=None, length=None):
    kwargs = {'Bucket': 'commoncrawl', 'Key': key_name}

    # Get the WARC file, optionally with offset and length
    if offset is not None:
        kwargs['Range'] = f'bytes={offset}-{offset + length}'  # First last offset inclusive

    s3_obj = s3.get_object(**kwargs)

    return s3_obj


def process_archive(key_name, s3_obj_body):
    # Iterate over the WARC content (must save the iterator to get the offset and length)
    it = ArchiveIterator(s3_obj_body)
    for rec in it:
        if rec.rec_type == 'response':
            # Get content
            cont = rec.content_stream().read()

            # Get offset and length (Must be put after reading the content!)
            offset = it.get_record_offset()
            length = it.get_record_length()

            # Return the result
            yield f'{" ".join(process_record(key_name, offset, length, rec, cont))}\n'

    print('DONE', key_name, file=sys.stderr, flush=True)


def process_record(key_name, offset, length, rec, content):
    # Get metadata
    status_code = rec.http_headers.get_statuscode()
    download_date = rec.rec_headers.get_header('WARC-Date')
    url = rec.rec_headers.get_header('WARC-Target-URI')
    orig_mime = rec.http_headers.get_header('content-type')

    # Compute extra metadata for simplicity
    urlkey = surt(url)
    server = urlparse(url).netloc

    # Get the required data out from the original cont!
    libmagic_mime = magic_from_buffer(content, mime=True)

    # This is a resource hog, disabled until further decision
    # res = m.identify_bytes(cont)
    # magicka_mime = res.output.mime_type
    # magicka_score = res.output.score

    # Try to extract text from HTML
    try:
        soup = BeautifulSoup(content, 'lxml')
        text_cont = soup.get_text()
    except MarkupResemblesLocatorWarning as e:
        print('MarkupResemblesLocatorWarning', e, key_name, offset, length, file=sys.stderr, flush=True)
        # Too short to extract anything
        try:
            text_cont = content.decode('UTF-8')
        except UnicodeDecodeError:
            print('UnicodeDecodeError', e, key_name, offset, length, file=sys.stderr, flush=True)
            text_cont = ''
    except XMLParsedAsHTMLWarning as e:
        print('Parsing as XML', e, key_name, offset, length, file=sys.stderr, flush=True)
        soup = BeautifulSoup(content, 'lxml-xml')
        text_cont = soup.get_text()

    # Strip the extracted content and compute further metadata from it
    text_cont_stripped = re.sub(r'\s+', ' ', text_cont.strip())
    content_length = len(text_cont_stripped)
    no_of_words = len(text_cont_stripped.split())
    detected_languages = {e.language.name: e.value
                          for e in detector.compute_language_confidence_values(text_cont_stripped)
                          if e.value >= 0.001}  # If >= 0.1% confident...

    metadata = {'key_name': key_name,
                'offset': offset,
                'length': length,
                'server': server,
                'url': url,
                'download_date': download_date,
                'status': status_code,
                'mime': orig_mime,
                'detected_mime': libmagic_mime,  # {'libmagic': libmagic_mime,
                # 'magicka': {'mime': magicka_mime, 'score': magicka_score}},
                'net_content_length': content_length,
                'net_no_of_words': no_of_words,
                'detected_langs': detected_languages,
                }

    # Return the results for CDXJ format
    return urlkey, iso_date_to_timestamp(download_date), json.dumps(metadata, ensure_ascii=False)


if __name__ == '__main__':
    main()
