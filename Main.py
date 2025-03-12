from pyspark import SparkContext
from mrjob.job import MRJob
sc = SparkContext.getOrCreate()

text_file = sc.textFile("coursegrades.txt")


class averageGrade(MRJob):

    def mapper(self, _, line):

        year, course_name, student_grade, university_name = line.split(',')

        yield course_name, int(student_grade)

    def reducer(self, course_name, grades):

        grades_list = list(grades)
        average_grade = sum(grades_list) / len(grades_list)
        yield course_name, average_grade

    def finalReducer(self, course_name, average_grade):
        max_average = -1
        max_course = ""

        for average, course_name in average_grade.items():
            if average > max_average:
                max_average = average
                max_course = course_name

        yield max_course, max_average

averageGrade.run()

