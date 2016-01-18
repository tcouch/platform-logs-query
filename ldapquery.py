from secrets.ldaplogs import Credentials as Creds
import ldap, sys

class LDAPCon:
    Credentials = Creds()
    def __init__(self,user = Credentials.user,
                 passwd = Credentials.passwd,
                 server = Credentials.server,
                 bind_user = Credentials.bind_user):
        try:
            self.connection = ldap.initialize(server)
            try:
                self.connection.bind_s(bind_user, passwd)
            except ldap.INVALID_CREDENTIALS:
                print "Your username or password is incorrect."
		sys.exit()
	except ldap.LDAPError, e:
            if type(e.message) == dict and e.message.has_key('desc'):
                print e.message['desc']
            else:
                print e
            sys.exit()

    def query_user(self,username,base = Credentials.base):
        filterstr = "cn=%s" % username
        return self.connection.search_ext_s(base,ldap.SCOPE_SUBTREE,filterstr,['department','sn','givenName'])

