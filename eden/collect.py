"""Functions for data web scrapping."""

from bs4 import BeautifulSoup


def parse_cities(self, response):
    """
    Scrapes site for a list of all cities.

    For later scraping we need a complete list of all city names

    Parameters
    ----------
    with_attribution : bool, Optional, default: True
        Set whether or not to display who the quote is from.

    Returns
    -------
    quote : str
        Compiled string including quote and optional attribution.
    """
    cities = response.css('div.col-md-4')
    for city in cities:
        yield {
            'name': city.css('u::text').get()
        }


if __name__ == "__main__":
    # Do something if this file is invoked on its own
    parse_cities()
