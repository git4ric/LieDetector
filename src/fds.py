import random
import datetime

comma = ", "

def generateRaw(subjects, topic, data):

    for i in subjects:
        for j in topic:
            data.write(i + comma + j + comma + '\n')


def generateStream(tdata, sdata):

    date = datetime.datetime(2016, 7, 1)
    for line in sdata:
        frequent = 7
        notfrequent = 3
        dice = -1
        date += datetime.timedelta(days=1)
        if line.startswith("Trump") or line.startswith("Bernie") or line.startswith("Obama"):
            dice = random.randint(notfrequent, frequent)

        elif line.startswith("Paul") or line.startswith("Hillary"):
            dice = random.randint(1, notfrequent)

        content = line.split(",")
        randomTopics = ['A','B','C','D','E','F','G','H','I'] #Have length of this greater than 'frequent'

        while dice >= 0:
            tdata.write(str(date.date()) + comma + line)
            tdata.write(str(date.date()) + comma + content[0] + comma + randomTopics[dice] + comma + content[2])
            dice -= 1



if __name__ == "__main__":

    _generateRaw = 0
    _generateStream = 1

    if _generateRaw:
        data = open("../data/targetdata.orig",'w')
        subjects = ['Trump', 'Paul Ryan', 'Hillary', 'Bernie', 'Obama']
        topics = ['Guns', 'Abortion', 'Immigration', 'Boeing',
                  'TPP', 'Wars', 'Social Programs', 'Raise Taxes',
                  'Cuba', 'Medicare', 'Eminent Domain', 'LGTBQ']

        generateRaw(subjects, topics, data)
        data.close()
        # After this you need to manually fill in stance
        # for each subject

    elif _generateStream:
        tdata = open("../data/targetdata_stream.orig",'w')
        sdata = open("../data/targetdatafilled.orig")

        generateStream(tdata, sdata)
        tdata.close()
        sdata.close()

        lines = open("../data/targetdata_stream.orig").readlines()
        random.shuffle(lines)
        open("../data/targetdata_stream.orig", 'w').writelines(lines)