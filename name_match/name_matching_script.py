from fuzzysearch import find_near_matches
from fuzzywuzzy import process
from ast import literal_eval

class name_matching:

    def __init__(self, user_name_full, user_name_mid, company_name, dba, external_account_name):
        self.user_name_full = user_name_full
        self.user_name_mid = user_name_mid
        self.company_name = company_name
        self.dba = dba
        self.external_account_name = external_account_name


    def fuzzy_extract_lib4(self, qs, ls, threshold):
        for word, _ in process.extractBests(qs, (ls,), score_cutoff=threshold):
            for match in find_near_matches(qs, word, max_l_dist=4):
                match = word[match.start:match.end]
                index = ls.find(match) +1
                yield (match, index)

    def fuzzy_extract_lib3(self, qs, ls, threshold):
        for word, _ in process.extractBests(qs, (ls,), score_cutoff=threshold):
            for match in find_near_matches(qs, word, max_l_dist=3):
                match = word[match.start:match.end]
                index = ls.find(match) +1
                yield (match, index)
                
    def fuzzy_extract_lib2(self, qs, ls, threshold):
        for word, _ in process.extractBests(qs, (ls,), score_cutoff=threshold):
            for match in find_near_matches(qs, word, max_l_dist=2):
                match = word[match.start:match.end]
                index = ls.find(match) +1
                yield (match, index)
                
    def fuzzy_extract_lib1(self, qs, ls, threshold):
        for word, _ in process.extractBests(qs, (ls,), score_cutoff=threshold):
            for match in find_near_matches(qs, word, max_l_dist=1):
                match = word[match.start:match.end]
                index = ls.find(match) +1
                yield (match, index)
                
    def fuzzy_extract(self, qs, ls, threshold):
        for word, _ in process.extractBests(qs, (ls,), score_cutoff=threshold):
            for match in find_near_matches(qs, word, max_l_dist=0):
                match = word[match.start:match.end]
                index = ls.find(match) +1
                yield (match, index)


    def get_result(self):

        if self.external_account_name == "Empty":
            return_text = 'External_Account_Name_Null'
        
        k=0

        if self.external_account_name != "Empty":
            external_account_names_1 = str(self.external_account_name).strip().upper().replace('\n', '').replace('  "','"')
            external_account_names_1 = literal_eval(external_account_names_1)

            company_name=self.company_name.upper()
            company_name_split = company_name.strip().split(' ')

            user_name_full = str(self.user_name_full).replace('-',' ') 
            user_name_full = literal_eval(user_name_full)
            user_name_mid = str(self.user_name_mid).replace('-',' ')
            user_name_mid = literal_eval(user_name_mid)

            dba = self.dba

            if self.dba != 'Empty':
                dba_name_split = self.dba.split(' ')

            
            for i in external_account_names_1:
                external_account_names_split = i.replace('-',' ').split(' ')

                if len(i)>=13:
                    for user_name_1 in user_name_full:
                        if len(user_name_1)>=9:
                            for match,index in self.fuzzy_extract_lib4(user_name_1.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib4(i.strip('-,.)(').upper(), user_name_1.strip('-,.)(').upper(), 50):
                                k=k+1
                
                
                    for user_name_2 in user_name_mid:
                        if len(user_name_2)>=9:
                            for match,index in self.fuzzy_extract_lib4(user_name_2.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib4(i.strip('-,.)(').upper(), user_name_2.strip('-,.)(').upper(), 50):
                                k=k+1
                
                    if len(company_name)>=9:
                        for match,index in self.fuzzy_extract_lib4(company_name.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                            k=k+1
                            

                        for match,index in self.fuzzy_extract_lib4(i.strip('-,.)(').upper(), company_name.strip('-,.)(').upper(), 50):
                            k=k+1
                            

                    if dba != 'Empty':
                        if len(dba)>=5:
                            for match,index in self.fuzzy_extract_lib4(i.strip('-,.)(').upper(), dba.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib4(dba.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                                k=k+1



                if len(i)>=10 and len(i)<=12:
                    for user_name_1 in user_name_full:
                        if len(user_name_1)>=7:
                            for match,index in self.fuzzy_extract_lib3(user_name_1.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib3(i.strip('-,.)(').upper(), user_name_1.strip('-,.)(').upper(), 50):
                                k=k+1
                
                
                    for user_name_2 in user_name_mid:
                        if len(user_name_2)>=7:
                            for match,index in self.fuzzy_extract_lib3(user_name_2.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib3(i.strip('-,.)(').upper(), user_name_2.strip('-,.)(').upper(), 50):
                                k=k+1
                
                    if len(company_name)>=7:
                        for match,index in self.fuzzy_extract_lib3(company_name.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                            k=k+1
                            

                        for match,index in self.fuzzy_extract_lib3(i.strip('-,.)(').upper(), company_name.strip('-,.)(').upper(), 50):
                            k=k+1
                            

                    if dba != 'Empty':
                        if len(dba)>=5:
                            for match,index in self.fuzzy_extract_lib3(i.strip('-,.)(').upper(), dba.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib3(dba.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                                k=k+1



                if len(i)>=6 and len(i)<=9:
                    for user_name_1 in user_name_full:
                        for match,index in self.fuzzy_extract_lib2(user_name_1.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                            k=k+1

                        for match,index in self.fuzzy_extract_lib2(i.strip('-,.)(').upper(), user_name_1.strip('-,.)(').upper(), 50):
                            k=k+1
                
                
                    for user_name_2 in user_name_mid:
                        for match,index in self.fuzzy_extract_lib2(user_name_2.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                            k=k+1

                        for match,index in self.fuzzy_extract_lib2(i.strip('-,.)(').upper(), user_name_2.strip('-,.)(').upper(), 50):
                            k=k+1
                
                    if len(company_name)<11:
                        for match,index in self.fuzzy_extract_lib2(company_name.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                            k=k+1

                        for match,index in self.fuzzy_extract_lib2(i.strip('-,.)(').upper(), company_name.strip('-,.)(').upper(), 50):
                            k=k+1

                    if dba != 'Empty':
                        if len(dba)>=5:
                            for match,index in self.fuzzy_extract_lib2(i.strip('-,.)(').upper(), dba.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib2(dba.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                                k=k+1

                

                if len(i)<=5:
                    for user_name_1 in user_name_full:
                        for match,index in self.fuzzy_extract_lib1(user_name_1.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                            k=k+1

                        for match,index in self.fuzzy_extract_lib1(i.strip('-,.)(').upper(), user_name_1.strip('-,.)(').upper(), 50):
                            k=k+1
                
                
                    for user_name_2 in user_name_mid:
                        for match,index in self.fuzzy_extract_lib1(user_name_2.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                            k=k+1

                        for match,index in self.fuzzy_extract_lib1(i.strip('-,.)(').upper(), user_name_2.strip('-,.)(').upper(), 50):
                            k=k+1

                    if len(company_name)<7:
                        for match,index in self.fuzzy_extract_lib1(company_name.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                            k=k+1

                        for match,index in self.fuzzy_extract_lib1(i.strip('-,.)(').upper(), company_name.strip('-,.)(').upper(), 50):
                            k=k+1

                    if dba != 'Empty':
                        if len(dba)>=5:
                            for match,index in self.fuzzy_extract_lib1(i.strip('-,.)(').upper(), dba.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib1(dba.strip('-,.)(').upper(), i.strip('-,.)(').upper(), 50):
                                k=k+1
                
                                
                if k==0:
                    i1 = i.strip().split(' ')
                    i2 = i1[0] + ' ' + i1[-1]

                    if len(i2)>=13:
                        for user_name_1 in user_name_full:
                            if len(user_name_1)>=9:
                                for match,index in self.fuzzy_extract_lib4(user_name_1.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                    k=k+1

                                for match,index in self.fuzzy_extract_lib4(i2.strip('-,.)(').upper(), user_name_1.strip('-,.)(').upper(), 50):
                                    k=k+1

                            user_name_3 = user_name_1.strip().split(' ')
                            user_name_3 = user_name_3[0] + ' ' + user_name_3[-1]

                            if len(user_name_3)>=9:
                                for match,index in self.fuzzy_extract_lib4(user_name_3.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                    k=k+1

                                for match,index in self.fuzzy_extract_lib4(i2.strip('-,.)(').upper(), user_name_3.strip('-,.)(').upper(), 50):
                                    k=k+1
                    
                    
                        for user_name_2 in user_name_mid:
                            if len(user_name_2)>=9:
                                for match,index in self.fuzzy_extract_lib4(user_name_2.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                    k=k+1

                                for match,index in self.fuzzy_extract_lib4(i2.strip('-,.)(').upper(), user_name_2.strip('-,.)(').upper(), 50):
                                    k=k+1
                    
                        if len(company_name)>=9:
                            for match,index in self.fuzzy_extract_lib4(company_name.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                k=k+1
                                

                            for match,index in self.fuzzy_extract_lib4(i2.strip('-,.)(').upper(), company_name.strip('-,.)(').upper(), 50):
                                k=k+1
                                

                        if dba != 'Empty':
                            if len(dba)>=5:
                                for match,index in self.fuzzy_extract_lib4(i2.strip('-,.)(').upper(), dba.strip('-,.)(').upper(), 50):
                                    k=k+1

                                for match,index in self.fuzzy_extract_lib4(dba.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                    k=k+1



                    if len(i2)>=10 and len(i2)<=12:
                        for user_name_1 in user_name_full:
                            if len(user_name_1)>=7:
                                for match,index in self.fuzzy_extract_lib3(user_name_1.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                    k=k+1

                                for match,index in self.fuzzy_extract_lib3(i2.strip('-,.)(').upper(), user_name_1.strip('-,.)(').upper(), 50):
                                    k=k+1
                            
                            user_name_3 = user_name_1.strip().split(' ')
                            user_name_3 = user_name_3[0] + ' ' + user_name_3[-1]

                            if len(user_name_3)>=7:
                                for match,index in self.fuzzy_extract_lib3(user_name_3.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                    k=k+1

                                for match,index in self.fuzzy_extract_lib3(i2.strip('-,.)(').upper(), user_name_3.strip('-,.)(').upper(), 50):
                                    k=k+1
                    
                    
                        for user_name_2 in user_name_mid:
                            if len(user_name_2)>=7:
                                for match,index in self.fuzzy_extract_lib3(user_name_2.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                    k=k+1

                                for match,index in self.fuzzy_extract_lib3(i2.strip('-,.)(').upper(), user_name_2.strip('-,.)(').upper(), 50):
                                    k=k+1
                    
                        if len(company_name)>=7:
                            for match,index in self.fuzzy_extract_lib3(company_name.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                k=k+1
                                

                            for match,index in self.fuzzy_extract_lib3(i2.strip('-,.)(').upper(), company_name.strip('-,.)(').upper(), 50):
                                k=k+1
                                

                        if dba != 'Empty':
                            if len(dba)>=5:
                                for match,index in self.fuzzy_extract_lib3(i2.strip('-,.)(').upper(), dba.strip('-,.)(').upper(), 50):
                                    k=k+1

                                for match,index in self.fuzzy_extract_lib3(dba.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                    k=k+1



                    if len(i2)>=6 and len(i2)<=9:
                        for user_name_1 in user_name_full:
                            for match,index in self.fuzzy_extract_lib2(user_name_1.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib2(i2.strip('-,.)(').upper(), user_name_1.strip('-,.)(').upper(), 50):
                                k=k+1

                            
                            user_name_3 = user_name_1.strip().split(' ')
                            user_name_3 = user_name_3[0] + ' ' + user_name_3[-1]

                            
                            for match,index in self.fuzzy_extract_lib2(user_name_3.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib2(i2.strip('-,.)(').upper(), user_name_3.strip('-,.)(').upper(), 50):
                                k=k+1
                    
                    
                        for user_name_2 in user_name_mid:
                            for match,index in self.fuzzy_extract_lib2(user_name_2.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib2(i2.strip('-,.)(').upper(), user_name_2.strip('-,.)(').upper(), 50):
                                k=k+1

                        if len(company_name)<11:
                            for match,index in self.fuzzy_extract_lib2(company_name.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib2(i2.strip('-,.)(').upper(), company_name.strip('-,.)(').upper(), 50):
                                k=k+1

                        if dba != 'Empty':
                            if len(dba)>=5:
                                for match,index in self.fuzzy_extract_lib2(i2.strip('-,.)(').upper(), dba.strip('-,.)(').upper(), 50):
                                    k=k+1

                                for match,index in self.fuzzy_extract_lib2(dba.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                    k=k+1

                    

                    if len(i2)<=5:
                        for user_name_1 in user_name_full:
                            for match,index in self.fuzzy_extract_lib1(user_name_1.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib1(i2.strip('-,.)(').upper(), user_name_1.strip('-,.)(').upper(), 50):
                                k=k+1

                            
                            user_name_3 = user_name_1.strip().split(' ')
                            user_name_3 = user_name_3[0] + ' ' + user_name_3[-1]

                            
                            for match,index in self.fuzzy_extract_lib1(user_name_3.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib1(i2.strip('-,.)(').upper(), user_name_3.strip('-,.)(').upper(), 50):
                                k=k+1
                    
                    
                        for user_name_2 in user_name_mid:
                            for match,index in self.fuzzy_extract_lib1(user_name_2.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib1(i2.strip('-,.)(').upper(), user_name_2.strip('-,.)(').upper(), 50):
                                k=k+1

                        if len(company_name)<7:
                            for match,index in self.fuzzy_extract_lib1(company_name.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                k=k+1

                            for match,index in self.fuzzy_extract_lib1(i2.strip('-,.)(').upper(), company_name.strip('-,.)(').upper(), 50):
                                k=k+1

                        if dba != 'Empty':
                            if len(dba)>=5:
                                for match,index in self.fuzzy_extract_lib1(i2.strip('-,.)(').upper(), dba.strip('-,.)(').upper(), 50):
                                    k=k+1

                                for match,index in self.fuzzy_extract_lib1(dba.strip('-,.)(').upper(), i2.strip('-,.)(').upper(), 50):
                                    k=k+1

                if k==0:
                    for user_name_1 in user_name_full:
                        user_name_split = user_name_1.replace('-',' ').split(' ')
                        for a in user_name_split:
                            for b in external_account_names_split:
                                if len(a)>=3 and len(b)>=3:
                                    for match,index in self.fuzzy_extract(a.strip('-,.)(').upper(), b.strip('-,.)(').upper(), 100):
                                        k=k+1
                                    
                if k==0:
                    exceptions = ['TECHNOLOGIES','SOLUTIONS','REVENUE','SOLUTION','TECHNOLOGY']   # Exceptions for Company Name
                    for a in company_name_split:
                        if ((a.strip('-,.)(').upper() != 'TECHNOLOGIES') & (a.strip('-,.)(').upper() != 'SOLUTIONS') & (a.strip('-,.)(').upper() != 'SOLUTION') & (a.strip('-,.)(').upper() != 'TECHNOLOGY') & (a.strip('-,.)(').upper() != 'REVENUE')):
                            for b in external_account_names_split:
                                if len(a)>=5 and len(b)>=5:
                                    for match,index in self.fuzzy_extract(a.strip('-,.)(').upper(), b.strip('-,.)(').upper(), 100):
                                        k=k+1
                                    
                if k==0:
                    if dba != 'Empty':
                        for a in dba_name_split:
                            for b in external_account_names_split:
                                if len(a)>=5 and len(b)>=5:
                                    for match,index in self.fuzzy_extract(a.strip('-,.)(').upper(), b.strip('-,.)(').upper(), 100):
                                        k=k+1

            if k>0:
                return_text = 'Name_Matched'
            if k==0:
                return_text = 'Name_Mismatch'

        return return_text
    

