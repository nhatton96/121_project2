import logging
from datamodel.search.NhtonZwalls_datamodel import NhtonZwallsLink, OneNhtonZwallsUnProcessedLink, add_server_copy, get_downloaded_content
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter, ServerTriggers
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4
import numpy as np
import os.path
from urlparse import urlparse, parse_qs, urljoin
from uuid import uuid4

from lxml import html,etree

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
        self.sub = dict()
        self.maxOut = 0
        self.maxUrl = ""


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
            links, tempsub, numlink = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(NhtonZwallsLink(l))                
            for k, v in tempsub.iteritems():
                if k:
                    if ".ics.uci.edu" in k:
                        self.sub[k] = self.sub.get(k,0) + v

            if numlink > self.maxOut:
                self.maxOut = numlink
                self.maxUrl = link.full_url

    def shutdown(self):
        if os.path.exists("outFile.txt"):
            print("Update outFile ... Done!")
            tempsub = np.genfromtxt('outFile.txt', delimiter=None, usecols=0, dtype=str)
            tempnum = np.genfromtxt('outFile.txt', delimiter=None)[:,1:]
            length = len(tempsub) - 1
            for i in range(length):
                self.sub[tempsub[i]] = self.sub.get(tempsub[i],0) + tempnum[i]
            if self.maxOut < tempnum[-1]:
                self.maxOut = tempnum[-1]
                self.maxUrl = tempsub[-1]

        file = open("outFile.txt",'w')   
        for sdm, amount in self.sub.iteritems():
            file.write("%s %d \n" % (sdm,amount))
    
        file.write("%s %d \n" % (self.maxUrl,self.maxOut))
        file.close() 

        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
def extract_next_links(rawDataObj):
    tempsub = dict()
    outputLinks = []
    url = rawDataObj.url
    if rawDataObj.is_redirected:
        url = rawDataObj.final_url
    
    if not rawDataObj.content:
        return outputLinks, tempsub, 0
    dom =  html.fromstring(rawDataObj.content)
    
    for link in dom.xpath('//a/@href'):
        sub_domain = urlparse(link).hostname
        tempsub[sub_domain] = tempsub.get(sub_domain, 0) + 1
        abs_url = urljoin(url, link)
        abs_url = abs_url.encode('utf-8')
        if abs_url != url:
            outputLinks.append(abs_url)
    num_link = len(outputLinks)
    return outputLinks, tempsub, num_link

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
            + ")$", parsed.path.lower()) \
            and not re.match("|^.*calendar.*|^.*(/misc|/sites|/all|/themes|/modules|/profiles|/css|/field|/node|/theme){3}.*|^.*?(/.+?/).*?\1.*|^.*?/(.+?/)\2.*", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False