import part1_create_tables
import part1_insert_data
import part2
import pandas as pd



def main():
    
   
    ans="" 
    while (ans != "n"):
        ans = input("Do you want to insert data into MySQL database? (y/n)")
        if(ans == "y"):
            part1_create_tables.main()
            part1_insert_data.main()
    
    
    program = part2.Program()
    all_queries = part2.SQLQueries()
    
    try:    
            print("Running queries...\n")
            
            print("\nTask 1:\n")
            for table_name, query in all_queries.part1.items():
                print(f"Table: {table_name}\n")
                program.fetch_data(query, table_name)

            print("\nTask 2.1:\n")
            for table_name, query in all_queries.task1.items():
                print(f"Table: {table_name}\n")
                program.fetch_data(query, table_name)

            print("\nTask 2.2:")
            print("Average number of activties per user:\n")
            program.cursor.execute(all_queries.task2_init)
            program.fetch_data(all_queries.task2)

            print("\nTask 2.3:")
            print("Top 20 users with most activities:\n")
            program.fetch_data(all_queries.task3)

            print("\nTask 2.4:")
            print("Users who have taken a taxi:\n")
            program.fetch_data(all_queries.task4)

            print("\nTask 2.5:")
            print("Types of transportation modes and their activities count:\n")
            program.fetch_data(all_queries.task5)

            print("\nTask 2.6a:")
            print("The year with the most activities:\n")
            program.fetch_data(all_queries.task6a)

            print("\nTask 2.6b:")
            print("The year with the most recorded hours:\n")
            program.fetch_data(all_queries.task6b)

            print("\nTask 2.7:")
            print("Total distance walked in 2008 by user with id=112:\n")
            track_points, columns = program.fetch_data_with_columns(all_queries.task7, print_results=False)
            track_points = pd.DataFrame(track_points, columns=columns).set_index('id')
            print(program.odometer(track_points))

            print("\nTask 2.8:")
            print("Top 20 users who have gained the most altitude:\n")
            track_points, columns = program.fetch_data_with_columns(all_queries.task8, print_results=False)
            track_points = pd.DataFrame(track_points, columns=columns)
            altitude_df = program.altitude_odometer(track_points)
            print(altitude_df.nlargest(20, 'altitude_gained'))

            print("\nTask 2.9:")
            print("Invalid activities per user:\n")
            date_points, columns = program.fetch_data_with_columns(all_queries.task9, print_results=False)
            date_points = pd.DataFrame(date_points, columns=columns)
            invalid_df = program.invalid_activities(date_points)
            pd.set_option('display.max_rows', None)
            print(invalid_df)

            print("\nTask 2.10:")
            print("Users who have tracked an activity in the Forbidden City of Beijing:\n")
            program.fetch_data(all_queries.task10)

            print("\nTask 2.11:")
            print("Most used transportation mode for all users:")
            trans_modes, columns = program.fetch_data_with_columns(all_queries.task11, print_results=False)
            trans_modes = pd.DataFrame(trans_modes, columns=columns)
            print(program.most_used_transportation(trans_modes))
            
            

    except Exception as e:
        print("ERROR: Failed to use database:", e)
        
    finally:
        if program:
            program.connection.close_connection()
    
        
    

if __name__ == "__main__":
    main()