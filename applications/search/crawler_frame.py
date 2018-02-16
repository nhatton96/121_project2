import logging
from datamodel.search.NhtonZwalls_datamodel import NhtonZwallsLink, OneNhtonZwallsUnProcessedLink, add_server_copy, get_downloaded_content
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter, ServerTriggers
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs, urljoin
from uuid import uuid4

from lxml import html,etree

sub = dict()
maxOut = 0
maxUrl = ""


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(NhtonZwallsLink)
@GetterSetter(OneNhtonZwallsUnProcessedLink)
@ServerTriggers(add_server_copy, get_downloaded_content)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        self.app_id = "NhtonZwalls"
        self.frame = frame


    def initialize(self):
        self.count = 0
        l = NhtonZwallsLink("http://www.ics.uci.edu/")
        print l.full_url
        self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get(OneNhtonZwallsUnProcessedLink)
        if unprocessed_links:
            link = unprocessed_links[0]
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(NhtonZwallsLink(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
def extract_next_links(rawDataObj):
    outputLinks = []
    url = rawDataObj.url
    if rawDataObj.is_redirected:
        url = rawDataObj.final_url
    
    if not rawDataObj.content:
        return outputLinks
    dom =  html.fromstring(rawDataObj.content)
    
    for link in dom.xpath('//a/@href'):
        sub_domain = urlparse(link).hostname
        sub[sub_domain] += 1
        abs_url = urljoin(url, link)
        outputLinks.append(abs_url)
    num_link = len(outputLinks)
    if num_link > maxOut:
        maxOut = num_link
        maxUrl = url

    file = open("outFile",'w')
    for sdm, amount in sub.iteritems():
        file.write(sdm ,":", amount)
    
    file.write("Link with max out is:")
    file.write(maxUrl, ":", maxOut)
    file.close() 	
    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz"\
            + "|^.*calendar.*|^.*(/misc|/sites|/all|/themes|/modules|/profiles|/css|/field|/node|/theme){3}.*|^.*?(/.+?/).*?\1.*|^.*?/(.+?/)\2.*"
            + ")$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False