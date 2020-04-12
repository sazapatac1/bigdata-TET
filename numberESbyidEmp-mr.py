from mrjob.job import MRJob

class MRNumberESbyidEmp(MRJob):

    def mapper(self, _, line):
        #split every field of csv file
        try:
            idemp, economicSector, salary, year = line.split(',')
            yield int(idemp), economicSector
        except:
            pass

    def reducer(self, key, values):
        economicSectors = []
        for economicSector in values:
            economicSectors.append(economicSector)
        yield key, len(economicSectors)


if __name__ == '__main__':
    MRNumberESbyidEmp.run()