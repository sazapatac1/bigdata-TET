from mrjob.job import MRJob

class MREconomicSectorAvg(MRJob):

    def mapper(self, _, line):
        #split every field of csv file
        try:
            idemp, economicSector, salary, year = line.split(',')
            yield economicSector, int(salary)
        except:
            pass

    def reducer(self, key, values):
        salaries = []
        for salary in values:
            salaries.append(salary)
        yield key, sum(salaries)/len(salaries)


if __name__ == '__main__':
    MREconomicSectorAvg.run()
