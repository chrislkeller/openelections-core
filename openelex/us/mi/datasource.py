"""handles the process of taking source files and organizing for system"""

from openelex.base.datasource import BaseDatasource

class Datasource(BaseDatasource):
    """scopes out the class for taking source files and organizing for system"""

    def mappings(self, year=None):
        """
        return array of dicts containing source url and
        standardized filename for raw results file, along
        with other pieces of metadata.
        metadata dictionaries should contain the following items:
            * election
            * generated_filename
            * name
            * ocd_id
            * raw_url
        """
        mappings = []
        for years, elecs in self.elections(year).items():
            mappings.extend(self._build_metadata(elecs))
        return mappings


    def target_urls(self, year=None):
        """Get list of source data urls, optionally filtered by year"""
        return [item['raw_url'] for item in self.mappings(year)]


    def filename_url_pairs(self, year=None):
        """creates paring for file and url"""
        return [(item['generated_filename'], item['raw_url'])
                for item in self.mappings(year)]

    def mappings_for_url(self, url):
        return [mapping for mapping in self.mappings() if mapping['raw_url'] == url]

    def _build_metadata(self, elections):
        """create an array of dicts containing metadata"""
        meta = []
        wrong_num_links_msg = ("There should be 1 direct link for election " "{}, instead found {}")
        for election in elections:
            meta.append({
                "generated_filename": self._standardized_filename(election),
                "raw_url": election['direct_links'][0],
                "ocd_id": "ocd-division/country:us/state:{}".format(self.state),
                "name": self.state,
                "election": election['slug']
            })
        return meta
