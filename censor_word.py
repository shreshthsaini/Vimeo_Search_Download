''' 
    This file is used to censor profane words from given list of words 

'''

import re
from better_profanity import profanity

class censor_words():
    def __init__(self, list_words):
        self.list_words = list_words        

    def censor_words(self):
        '''
            Censoring profane words

            Args:
            - list_words (list): list of words to be censored

            Returns:
            - original_vs_cleaned (list): list of tuples of original and cleaned words 
        '''
        # Censoring profane words
        cleaned_words = [profanity.censor(word) for word in self.list_words]
        return cleaned_words
