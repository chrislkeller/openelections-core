"""
fetch module to download raw result files for your state
"""

import os
import os.path
import urlparse
from zipfile import ZipFile
from openelex.base.fetch import BaseFetcher
from openelex.us.mi.datasource import Datasource

class FetchResults(BaseFetcher):
    """
    overrides the fetch class
    """
    def __init__(self):
        """
        """
        super(FetchResults, self).__init__()
        self._datasource = Datasource()
        self._fetched = set()


    def fetch(self, url, fname=None, overwrite=False):
        """
        we keep track of urls we've already fetched in this run since
        there will be multiple output files mapped to a single zip
        file.  if we've already fetched this url, exit early.
        """
        if url in self._fetched:
            return
        if url.endswith('.zip'):
            zip_fname = self._local_zip_file_name(url)
            super(FetchResults, self).fetch(url, zip_fname, overwrite)
            self._extract_zip(url, zip_fname, overwrite, False)
        else:
            super(FetchResults, self).fetch(url, fname, overwrite)
        self._fetched.add(url)


    def _local_zip_file_name(self, url):
        """
        return a normalized local file name for a results zip file.
        we don't care too much about the format
        because we can delete the zip file later.
        """
        parsed = urlparse.urlsplit(url)
        fname = parsed.path.split('/')[-1]
        return os.path.join(self.cache.abspath, fname)


    def _extract_zip(self, url, zip_fname=None, overwrite=False, remove=True):
        """
        """
        if zip_fname is None:
            zip_fname = self._local_zip_file_name(url)
        with ZipFile(zip_fname, 'r') as zipf:
            for mapping in self._datasource.mappings_for_url(url):
                revised_filename = "%s" % (mapping["election"])
                extracted_file_name = os.path.join(self.cache.abspath, revised_filename)
                if overwrite or not os.path.exists(extracted_file_name):
                    zipf.extractall(extracted_file_name)
                    print "Added %s directory to cache" % (mapping["election"])
                else:
                    print "%s directory is cached" % (mapping["election"])
        if remove:
            os.remove(zip_fname)
