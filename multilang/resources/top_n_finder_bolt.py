import heapq
from collections import Counter

import storm


class TopNFinderBolt(storm.BasicBolt):
    # Initialize this instance
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context

        storm.logInfo("Counter bolt instance starting...")

        # TODO:
        # Task: set N
        self._N = 10
        pass
        # End

        # Hint: Add necessary instance variables and classes if needed
        self._counter = Counter()

    def process(self, tup):
        '''
        TODO:
        Task: keep track of the top N words
        Hint: implement efficient algorithm so that it won't be shutdown before task finished
              the algorithm we used when we developed the auto-grader is maintaining a N size min-heap
        '''
        word = tup.values[0]
        count = int(tup.values[1])

        if word not in self._counter:
            self._counter[word] = count
        elif count > self._counter[word]:
            self._counter[word] = count

        top_n_values = self._counter.most_common(self._N)

        top_n = "top-N"
        
        top_n_list = [x[0] for x in top_n_values]
        top_n_words = ", ".join(top_n_list)
        storm.emit([top_n, top_n_words])
        # End


# Start the bolt when it's invoked
TopNFinderBolt().run()
