'''
Created on Oct 20, 2016
@author: Rohan Achar
'''
from rtypes.pcc.attributes import dimension, primarykey
from rtypes.pcc.triggers import trigger, TriggerAction, TriggerTime
from rtypes.pcc.types.subset import subset
from rtypes.pcc.types.set import pcc_set
from rtypes.pcc.types.projection import projection
from rtypes.pcc.types.impure import impure
from datamodel.search.server_datamodel import Link, ServerCopy

@pcc_set
class NhtonZwallsLink(Link):
    USERAGENTSTRING = "NhtonZwalls"

    @dimension(str)
    def user_agent_string(self):
        return self.USERAGENTSTRING

    @user_agent_string.setter
    def user_agent_string(self, v):
        # TODO (rachar): Make it such that some dimensions do not need setters.
        pass


@subset(NhtonZwallsLink)
class NhtonZwallsUnprocessedLink(object):
    @staticmethod
    def __predicate__(l):
        return not (l.download_complete or l.error_reason)


@impure
@subset(NhtonZwallsUnprocessedLink)
class OneNhtonZwallsUnProcessedLink(NhtonZwallsLink):
    __limit__ = 1
    @staticmethod
    def __predicate__(l):
        return not (l.download_complete or l.error_reason)

@projection(NhtonZwallsLink, NhtonZwallsLink.url, NhtonZwallsLink.download_complete)
class NhtonZwallsProjectionLink(object):
    pass

@trigger(OneNhtonZwallsUnProcessedLink, TriggerTime.after, TriggerAction.read)
def get_downloaded_content(dataframe, new, old, current):
    server_copy = dataframe.get(ServerCopy, oid=current.url)
    if server_copy:
        current.copy_from(server_copy)


@trigger(NhtonZwallsLink, TriggerTime.after, TriggerAction.update)
def add_server_copy(dataframe, new, old, current):
    server_copy = dataframe.get(ServerCopy, oid=current.url)
    if not server_copy:
        dataframe.append(ServerCopy, ServerCopy(current))
