import pandas as pd
import time
import datetime
# Define file paths
data_test_path = r'C:\Users\User\OneDrive\Desktop\Impress\Google Ads Impress\Python Analysis\SQ_Labels\PQ Data All\Initial Data.xlsx'
sq_all_labels_path = r'C:\Users\User\OneDrive\Desktop\Impress\Google Ads Impress\Python Analysis\SQ_Labels\PQ Data Labels\SQ All Labels.xlsx'
output_path_template = r'C:\Users\User\OneDrive\Desktop\Impress\Google Ads Impress\Python Analysis\SQ_Labels\Results\Results_with_labels_aggregated_{datetime}.xlsx'

# Load the provided files
data_test = pd.read_excel(data_test_path)
sq_all_labels = pd.read_excel(sq_all_labels_path)

# Load custom order for Summed Group Labels, Summed Labels, and Standardized Term
order_summed_group_labels = pd.read_excel(sq_all_labels_path, sheet_name='Order Summed Group Labels')['Group Labels'].tolist()
order_summed_labels = pd.read_excel(sq_all_labels_path, sheet_name='Order Summed Labels')['Labels'].tolist()
order_standardized_term = pd.read_excel(sq_all_labels_path, sheet_name='Order Standardized Term')['Standardized Term'].tolist()

# Ensure all values in 'Uniq word' column are strings
sq_all_labels['Uniq word'] = sq_all_labels['Uniq word'].astype(str)

# Load stop words from sq_all_labels where Group label = 'Stop words'
stop_words_df = sq_all_labels[sq_all_labels['Group label'] == 'Stop words']
stop_words = set(stop_words_df['Uniq word'].str.lower().tolist())

# Function to remove stop words from search terms
def remove_stop_words_from_term(term):
    if isinstance(term, str):
        words = term.lower().split()
        words_filtered = [word for word in words if word not in stop_words]
        return ' '.join(words_filtered)
    else:
        return term

# Separate multi-word and single-word phrases, excluding Stop words
sq_all_labels_filtered = sq_all_labels[sq_all_labels['Group label'] != 'Stop words'].copy()
sq_all_labels_filtered['Word Count'] = sq_all_labels_filtered['Uniq word'].str.split().str.len()
multi_word_labels = sq_all_labels_filtered[sq_all_labels_filtered['Word Count'] > 1]
single_word_labels = sq_all_labels_filtered[sq_all_labels_filtered['Word Count'] == 1]

# Create a dictionary to map custom order to index
priority_dict_group = {label: idx for idx, label in enumerate(order_summed_group_labels)}
priority_dict_labels = {label: idx for idx, label in enumerate(order_summed_labels)}
priority_dict_term = {word: idx for idx, word in enumerate(order_standardized_term)}

# Function to standardize terms according to custom order
def standardize_term(term):
    if isinstance(term, str):
        # First remove stop words
        term_without_stop_words = remove_stop_words_from_term(term)
        if term_without_stop_words.strip():  # Check if there are any words left after removing stop words
            words = term_without_stop_words.lower().split()
            words_sorted = sorted(words, key=lambda x: priority_dict_term.get(x, len(priority_dict_term)))
            return ' '.join(words_sorted)
        else:
            return 'notfound'  # If only stop words were present
    else:
        return 'notfound'

# Standardize search terms in the data
data_test['Standardized Term'] = data_test['Search term'].apply(standardize_term)

# Define the columns for aggregation
sum_columns = ['Impr.', 'Clicks', 'Conversions', 'Cost', 'Bookings', 'Showup', 'PFP']

# Convert all columns to numeric
for col in sum_columns:
    data_test[col] = pd.to_numeric(data_test[col], errors='coerce')

# Ensure Month column is properly formatted as datetime
# Convert Excel serial dates to datetime if they are stored as integers
if data_test['Month'].dtype in ['int64', 'float64']:
    # Excel stores dates as number of days since 1899-12-30
    data_test['Month'] = pd.to_timedelta(data_test['Month'], unit='d') + pd.Timestamp('1899-12-30')
else:
    data_test['Month'] = pd.to_datetime(data_test['Month'], errors='coerce')

# Group by the standardized term and aggregate the data
aggregated_data = data_test.groupby(['Standardized Term', 'Month', 'Account name', 'Customer ID', 'Currency code']).agg(
    {col: 'sum' for col in sum_columns}
).reset_index()

# Create an empty DataFrame to store the results
results = aggregated_data.copy()

# Initialize columns in the results DataFrame based on example DataFrame, excluding Stop words
group_label_columns = sorted(set(sq_all_labels_filtered['Group label']).union({'Not Found'}), key=lambda x: priority_dict_group.get(x, len(priority_dict_group)))
label_columns = sorted(set(sq_all_labels_filtered['Label']).union({'Not Found Words'}), key=lambda x: priority_dict_labels.get(x, len(priority_dict_labels)))

# Create a DataFrame with zero-initialized columns
zero_data = pd.DataFrame(0, index=results.index, columns=group_label_columns + label_columns)
results = pd.concat([results, zero_data], axis=1)

# Create lists to store summed labels
summed_group_labels = []
summed_labels = []
not_found_terms = []

# Timing and progress tracking
total_rows = len(results)
start_time = time.time()

# Iterate through each standardized search term and compare words
for i, row in results.iterrows():
    term = row['Standardized Term']
    words = term.split()
    term_group_labels = []
    term_labels = []
    used_words = set()

    # Process multi-word phrases first
    for match in multi_word_labels.itertuples():
        uniq_words = match[3].lower()
        if uniq_words in term:
            term_group_labels.append(match[1])
            term_labels.append(match[2])
            used_words.update(uniq_words.split())
            results.at[i, match[1]] = 1
            results.at[i, match[2]] = 1

    # Process single words
    for match in single_word_labels.itertuples():
        uniq_word = match[3].lower()
        if uniq_word in words and uniq_word not in used_words:
            term_group_labels.append(match[1])
            term_labels.append(match[2])
            used_words.add(uniq_word)
            results.at[i, match[1]] = 1
            results.at[i, match[2]] = 1

    # Mark not found words (excluding stop words)
    not_found_words = [word for word in words if word not in used_words and word not in stop_words]
    if not_found_words:
        term_group_labels.append('Not Found')
        term_labels.append('Not Found Words')
        results.at[i, 'Not Found'] = 1
        results.at[i, 'Not Found Words'] = 1
        for word in not_found_words:
            not_found_terms.append((word, row['Impr.'], row['Clicks'], row['Conversions'],
                                    row['Cost'], row['Bookings'],
                                    row['Showup'], row['PFP'], row['Account name'],
                                    row['Customer ID'], row['Currency code']))

    # Remove duplicates and sort according to custom order
    term_group_labels = sorted(pd.Series(term_group_labels).unique(), key=lambda x: priority_dict_group.get(x, len(priority_dict_group)))
    term_labels = sorted(pd.Series(term_labels).unique(), key=lambda x: priority_dict_labels.get(x, len(priority_dict_labels)))

    summed_group_labels.append(' | '.join(term_group_labels))
    summed_labels.append(' | '.join(term_labels))

    # Print progress
    if (i + 1) % 100 == 0 or (i + 1) == total_rows:
        elapsed_time = time.time() - start_time
        remaining_time = elapsed_time * (total_rows - (i + 1)) / (i + 1)
        progress_percentage = (i + 1) / total_rows * 100
        print(f"Processed {i + 1}/{total_rows} rows ({progress_percentage:.2f}%) - Elapsed time: {elapsed_time:.2f}s - Remaining time: {remaining_time:.2f}s")

results['Summed Group Labels'] = summed_group_labels
results['Summed Labels'] = summed_labels

# Generate the output file name with the current date and time
current_datetime = datetime.datetime.now().strftime("%d.%m.%Y.%H_%M")
output_path = output_path_template.format(datetime=current_datetime)

# Drop duplicate columns
results = results.loc[:, ~results.columns.duplicated()]

# Save results and Not Found terms to a single Excel workbook
results = results[['Month', 'Standardized Term', 'Account name', 'Customer ID', 'Currency code'] + sum_columns + group_label_columns + label_columns + ['Summed Group Labels', 'Summed Labels']]
not_found_df = pd.DataFrame(not_found_terms, columns=['Word', 'Impr.', 'Clicks', 'Conversions', 'Cost', 'Bookings', 'Showup', 'PFP', 'Account name', 'Customer ID', 'Currency code']).groupby(['Word', 'Account name', 'Customer ID', 'Currency code']).sum().reset_index()
not_found_df = not_found_df.sort_values(by='Impr.', ascending=False)


with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    results.to_excel(writer, sheet_name='Results', index=False)
    not_found_df.to_excel(writer, sheet_name='Not Found', index=False)

    # Format Month column as MMM-YY (e.g., Oct-25)
    worksheet = writer.sheets['Results']
    from openpyxl.styles import numbers
    for row in range(2, len(results) + 2):  # Start from row 2 (skip header)
        cell = worksheet.cell(row=row, column=1)  # Column A (Month)
        cell.number_format = 'MMM-YY'


print(f"Script completed successfully! Results workbook saved to {output_path}")
print("Sheets generated: Results, Not Found")