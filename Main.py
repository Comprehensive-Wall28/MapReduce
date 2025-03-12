from pyspark import SparkContext

sc = SparkContext.getOrCreate()

print(" ")
print("#====== COURSE AVERAGES ======#")
print(" ")

# Read Data
text_file = sc.textFile("coursegrades.txt")

# Map the values to key Course and value Grade
course_grades = text_file.map(lambda line: line.split(',')) \
    .map(lambda parts: (parts[1].strip(), int(parts[2].strip())))

# Reduce the data, get the average of all grades
# .mapValues converts into list, groupByKey groups grades by course name
course_totals = course_grades.groupByKey() \
    .mapValues(list) \
    .map(lambda x: (x[0], sum(x[1]) / len(x[1]) if x[1] else 0))

# Find the Highest Average
highest_average = course_totals.max(key=lambda x: x[1])

# print
for course, average in course_totals.collect():
    print(f"Course: {course}, Average Grade: {average}")

print(f"Course with the highest average grade: {highest_average[0]} "
      f"with an average of: {highest_average[1]}")

print(" ")
print("#====== UNIVERSITY AVERAGES ======#")
print(" ")

#Map 
university_grades = text_file.map(lambda line: line.split(',')) \
    .map(lambda parts: (parts[3].strip(), int(parts[2].strip())))

#Find average for universities (reduce)
university_average = university_grades.groupByKey() \
    .mapValues(list) \
    .map(lambda x: (x[0], sum(x[1]) / len(x[1]) if x[1] else 0))

#Find highest average
highest_uni_average = university_average.max(key=lambda x: x[1])

#Print
for university, average in university_average.collect():
    print(f"University: {university}, Average Grade: {average}") 

print(f"University with the highest average grade: {highest_uni_average[0]} "
      f"with an average of: {highest_uni_average[1]}")

print(" ")
print("#====== TOP 3 GRADES PER YEAR ======#")
print(" ")

# Map the values to key Year and value Grade
year_grades = text_file.map(lambda line: line.split(',')) \
    .map(lambda parts: (parts[0].strip(), int(parts[2].strip())))

# Reduce the data to get top 3 grades for each year
def top_3_grades(grades):
    sorted_grades = sorted(grades, reverse=True)
    return sorted_grades[:3]

year_top_grades = year_grades.groupByKey() \
    .mapValues(list) \
    .mapValues(top_3_grades)

# Print the top 3 grades for each year
for year, grades in year_top_grades.collect():
    print(f"Year: {year}, Top 3 Grades: {grades}")

sc.stop()