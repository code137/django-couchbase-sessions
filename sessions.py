from django.conf import settings
from django.utils.encoding import force_text as force_unicode
from django.contrib.sessions.backends.base import CreateError, SessionBase
from couchbase.bucket import Bucket

class SessionStore(SessionBase):
    """
    A couchbase-based session store.
    """

    def __init__(self, session_key=None):
        super(SessionStore, self).__init__(session_key)
        host = settings.COUCHBASE_HOST
        bucket = settings.COUCHBASE_BUCKET
        self.server = Bucket('couchbase://' + host + '/' + bucket)

    @property
    def cache_key(self):
        return self._get_or_create_session_key()

    def load(self):
        try:
            session_data = self.server.get(
                self._get_or_create_session_key()
            )
            return session_data.value
        except:
            self._session_key = None
            return {}

    def create(self):
        while True:
            self._session_key = self._get_new_session_key()
            try:
                self.save(must_create=True)
            except CreateError:
                continue
            self.modified = True
            return


    def save(self, must_create=False):
        if self.session_key is None:
            return self.create()
        if must_create and self.exists(self._get_or_create_session_key()):
            raise CreateError
        if must_create:
            data = self._get_session(no_load=must_create)
            self.server.insert(
                self._get_or_create_session_key(),
                data
            )
        else:
            data = self._get_session(no_load=must_create)
            self.server.replace(
                self._get_or_create_session_key(),
                data
            )


    def exists(self, session_key):
        rv = self.server.get(session_key, quiet=True)
        return rv.success


    def delete(self, session_key=None):
        if session_key is None:
            if self.session_key is None:
                return
            session_key = self.session_key
        try:
            self.server.remove(session_key)
        except:
            pass


    @classmethod
    def clear_expired(cls):
        pass
