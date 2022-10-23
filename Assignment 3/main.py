from part1 import *
from part2 import *


def main():
    
    try:
        program = Part2Program()

        print("Part 2.1:", sep="\n")
        print("Total amount of users, activities and trackpoints in the dataset")
        program.part_1()

        print("\nPart 2.2:", sep="\n")
        print("Find the average number of activities per user")
        program.part_2()

        print("\nPart 2.3:", sep="\n")
        print("Find top 20 users with the highest number of activities")
        program.part_3()

        print("\nPart 2.4:", sep="\n")
        print("Find all users who have taken a taxi")
        program.part_4()

        print("\nPart 2.5:", sep="\n")
        print("Find activities per transportation mode")
        program.part_5()

        print("\nPart 2.6a:", sep="\n")
        print("Find the year with the most activities")
        program.part_6a()

        print("\nPart 2.7:", sep="\n")
        print("Find the year with the most recorded hours")
        program.part_7()

        print("\nPart 2.8:", sep="\n")
        print("The the top 20 users who gained the most altitude meters")
        program.part_8()
        
        print("\nPart 2.9:", sep="\n")
        print("Find all users who have invalid activities, and the number of invalid activities per user")
        program.part_9()

        print("\nPart 2.10:", sep="\n")
        print("Find all users who have tracked an activity in the Forbidden City of Beijing.")
        program.part_10()

        print("\nPart 2.11:", sep="\n")
        print("Find all users who have registered transportation mode and their most used transportation mode.")
        program.part_11()
    
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()
    

if __name__ == '__main__':
    init = input("Do you want to insest data into the database? (y/n): ")
    if( init == 'y'):
        insert_data()
    
    program = None
    
   