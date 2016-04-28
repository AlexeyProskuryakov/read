class StateObject(object):
    def __init__(self, global_state, hb_state):
        self.global_state = global_state
        self.hb_state = hb_state


class AspectState(object):
    def __init__(self, aspect, state):
        self.aspect = aspect
        self.state = state

    def to_dict(self):
        return self.__dict__

    @staticmethod
    def from_dict(dict):
        if "aspect" in dict and "state" in dict:
            return AspectState(**dict)
        raise Exception("can not create aspect state from this dict %s", dict)


class HeartBeatTask(object):
    def __init__(self, action, aspect, state=None):
        self.action = action
        self._state = AspectState(aspect, state)

    @property
    def text_state(self):
        return self._state.state

    @property
    def aspect(self):
        return self._state.aspect

    def to_dict(self):
        return dict(self._state.to_dict(), **{"action": self.action})

    @staticmethod
    def from_dict(dict):
        if "aspect" in dict and "state" in dict and "action" in dict:
            return HeartBeatTask(**dict)
        raise Exception("can not create hart beat task from dict %s", dict)
