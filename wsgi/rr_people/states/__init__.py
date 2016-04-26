class State(object):
    def __init__(self, global_state, hb_state):
        self.global_state = global_state
        self.hb_state = hb_state


class HeartBeatTask(object):
    def __init__(self, action, aspect, state=None):
        self.action = action
        self.aspect = aspect
        self.state = state


