from pyspark import SparkContext

sc = SparkContext.getOrCreate()

# Read Data
text_file = sc.textFile("coursegrades.txt")

#Map the values to key Course and value Grade
course_grades = text_file.map(lambda line: line.split(',')) \
    .map(lambda parts: (parts[1].strip(), int(parts[2].strip())))

#Reduce the data, get the average of all grades
course_totals = course_grades.groupByKey() \
    .mapValues(list) \
    .map(lambda x: (x[0], sum(x[1]) / len(x[1]) if x[1] else 0))

#Find the Highest Average
highest_average = course_totals.max(key=lambda x: x[1])

# Output the results (to Colab's output)
for course, average in course_totals.collect():
    print(f"Course: {course}, Average Grade: {average:.2f}")

print(f"Course with the highest average grade: {highest_average[0]} "
    f"with an average of: {highest_average[1]:.2f}")

# Stop SparkContext
sc.stop()