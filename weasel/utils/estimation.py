class MA:

    def __init__(self, k):
        self.k = k
        self.average = 0.0
        self.past_values = []

    def get_next_value(self, current_value):
        if len(self.past_values) > self.k:
            self.past_values.pop(0)
        self.past_values.append(current_value)

    def get_estimated_value(self):
        if len(self.past_values) == 0:
            return 0
        avg_0 = 0.0
        for i in self.past_values:
            avg_0 = avg_0 + i
        self.average = avg_0 / len(self.past_values)
        return self.average

    def reset(self):
        self.average = 0.0
        self.past_values = []

class EWMA(MA):

    def __init__(self, k, alpha):
        MA.__init__(self, k)
        if alpha > 1:
            alpha = 1.0
        if alpha < 0:
            alpha = 0
        self.alpha = alpha
        self.next_value = 0.0
        self.first = True

    def get_estimated_value(self):
        if len(self.past_values) == 0:
            return 0
        avg_0 = self.past_values[0]
        for i in reversed(self.past_values[1:]):
            avg_0 = self.alpha * i + (1.0 - self.alpha) * avg_0
        self.average = avg_0
        return self.average

    def reset(self):
        self.average = 0.0
        self.k = []
