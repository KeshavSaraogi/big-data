# The following informartion has been sourced from 'README.html' file within the dataset.

# -- All ratings are contained in the file ratings.dat. 
# -- Each line of this file represents one rating of one movie by one user, and has the following format:
# -- UserID::MovieID::Rating::Timestamp

import pandas as pd
filePath = '/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/ml-10M100K/ratings.dat'

data = pd.read_csv(filePath, engine = 'python', sep = '::', names = ['UserID', 'MovieID', 'Rating', 'Timestamp'])
print(data.head())

print(data.shape)
print(data.isnull().sum())
print(data.dtypes)

# -- Problem 1.a

# -- Divide the data to 5 almost equal size files and use the five files in the rest of the assignment. 

try:
    numFiles = 5
    numRows = len(data)
    # print(numRows)
    numSplit = numRows // numFiles
    # print(numSplit)

    for i in range(numFiles):
        start = i * numSplit
        if i == numFiles - 1:
            end = numRows
        else:
            end = (i + 1) * numSplit

        splitData = data.iloc[start:end]

        fileName = f"data_file_{i + 1}.csv"
        splitData.to_csv(fileName, index=False)
        print(f"{fileName} created.")

    print("All Files Have Been Created.")

except Exception as e:
    print(f"An error occurred: {e}")


# -- Problem 1.b

# -- Sort the data from the highest rating movie to the lowest one. 
# -- Measure how much time sorting takes. (6 points)
    # -- Don’t use sort function, and write the sort function yourself. 
    # -- Use sort function  

file_paths = [
    '/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/data_file_1.csv',
    '/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/data_file_2.csv',
    '/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/data_file_3.csv',
    '/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/data_file_4.csv',
    '/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/data_file_5.csv'
]

def merge_sort(arr, col_index):
    if len(arr) > 1:
        mid = len(arr) // 2
        L = arr[:mid]
        R = arr[mid:]

        merge_sort(L, col_index)
        merge_sort(R, col_index)

        i = j = k = 0

        while i < len(L) and j < len(R):
            if L[i][col_index] >= R[j][col_index]:
                arr[k] = L[i]
                i += 1
            else:
                arr[k] = R[j]
                j += 1
            k += 1

        while i < len(L):
            arr[k] = L[i]
            i += 1
            k += 1

        while j < len(R):
            arr[k] = R[j]
            j += 1
            k += 1


# -- Problem 1.b (continued)

import pandas as pd
import time

try:
    for file_path in file_paths:
        data = pd.read_csv(file_path)
        data_list = data.values.tolist()
        
        start_time = time.time()
        merge_sort(data_list, 2)
        custom_sort_time = time.time() - start_time
        sorted_custom = pd.DataFrame(data_list, columns = data.columns)
        
        start_builtin_time = time.time()
        sorted_builtin = data.sort_values(by='Rating', ascending=False)
        builtin_sort_time = time.time() - start_builtin_time
        
        print(f"File: {file_path}")
        print(f"Custom Merge Sort Time: {custom_sort_time:.4f} seconds")
        print(f"Built-in Sort Time: {builtin_sort_time:.4f} seconds\n")

except Exception as e:
    print(f"An error occurred: {e}")

# -- Problem 1.b (continued)

import csv
import time

# Function to read CSV files into a list
def read_csv_file(file_path):
    try:
        with open(file_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            data = []
            for row in reader:
                for item in row:
                    try:
                        data.append(int(item))
                    except ValueError:
                        continue
        return data
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return []

def merge_two_lists(list1, list2):
    merged_list = []
    i = j = 0
    
    while i < len(list1) and j < len(list2):
        if list1[i] < list2[j]:
            merged_list.append(list1[i])
            i += 1
        else:
            merged_list.append(list2[j])
            j += 1
    
    merged_list.extend(list1[i:])
    merged_list.extend(list2[j:])
    
    return merged_list

def merge_multiple_lists(lists):
    if not lists:
        return []
    
    while len(lists) > 1:
        new_lists = []
        for i in range(0, len(lists), 2):
            if i + 1 < len(lists):
                new_lists.append(merge_two_lists(lists[i], lists[i + 1]))
            else:
                new_lists.append(lists[i])
        lists = new_lists
    return lists[0]

try:
    file_paths = [
        "/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/data_file_1.csv",
        "/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/data_file_2.csv",
        "/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/data_file_3.csv",
        "/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/data_file_4.csv",
        "/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/data_file_5.csv"
    ]

    start_time = time.time()

    all_data = [read_csv_file(file) for file in file_paths]
    sorted_data = merge_multiple_lists(all_data)

    output_file = "/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/sorted_merged_file.csv"
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(sorted_data)

    end_time = time.time()

    time_taken = end_time - start_time
    print(f"Sorted and merged data has been saved to: {output_file}")
    print(f"Time taken to complete the process: {time_taken:.2f} seconds")

except Exception as e:
    print(f"An error occurred during the merge process: {e}")

# -- Problem 1.c


start_time = time.time()

movie_rating_counts = data.groupby('MovieID').size().sort_index()

plt.figure(figsize=(10, 6))
plt.plot(movie_rating_counts.index, movie_rating_counts.values, color='blue')
plt.fill_between(movie_rating_counts.index, movie_rating_counts.values, color='blue')
plt.title('Movie ID vs Number of Ratings')
plt.xlabel('Movies')
plt.ylabel('Frequency of Rating')
plt.grid(True)

plt.show()

end_time = time.time()
print(f"Time taken to create the histogram: {end_time - start_time:.4f} seconds")

# Problem 1.d


start_time = time.time()

plt.figure(figsize=(10, 6))
#getting the count of bins to use in part e
counts, bins, patches = plt.hist(data['Rating'], bins=10, edgecolor='black', color= 'blue')
plt.title('Distribution of Movie Ratings')
plt.xlabel('Rating')
plt.ylabel('Frequency')

plt.show()

end_time = time.time()

print(f"Time taken to create the histogram: {end_time - start_time:.4f} seconds")

# Lowest three bins correspond to ratings between 0.5 and 1.85
low_bin_min = bins[0]  
low_bin_max = bins[3]  

low_ratings = data[(data['Rating'] >= low_bin_min) & (data['Rating'] < low_bin_max)]

plt.figure(figsize=(10, 6))
plt.hist(low_ratings['Rating'], bins=3, edgecolor='gray', color = 'blue')
plt.title('Histogram of Ratings in Lowest Three Bins (0.5 - 1.85)')
plt.xlabel('Rating')
plt.ylabel('Frequency')
plt.show()

# Top three bins correspond to ratings between 3.65 and 5.0
high_bin_min = bins[-4]  
high_bin_max = bins[-1] 

high_ratings = data[(data['Rating'] >= high_bin_min) & (data['Rating'] <= high_bin_max)]

plt.figure(figsize=(10, 6))
plt.hist(high_ratings['Rating'], bins=3, edgecolor='gray', color='blue')
plt.title('Histogram of Ratings in Top Three Bins (3.65 - 5.0)')
plt.xlabel('Rating')
plt.ylabel('Frequency')
plt.show()
