import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

df = pd.read_csv("/home/hadoop/final_project/csv_temp/global_product_sales.csv")
item_list = pd.read_csv("/home/hadoop/final_project/csv_temp/df_item_fact_table.csv")

df['item_id']=df['item_id'].astype('str').str.strip()
df['bill_no']=df['bill_no'].astype('str')

basket=(df[df['country_id']==28]
        .groupby(['bill_no','item_id'])['quantity']
        .sum().unstack().reset_index().fillna(0)
        .set_index('bill_no'))

def encode_units(x):
  if x<=0:
    return 0
  if x>=1:
    return 1
  
basket_sets=basket.map(encode_units)
frequent_itemsets=apriori(basket_sets,min_support=0.03,use_colnames=True)

rules = association_rules(frequent_itemsets, metric='lift', min_threshold=1)

top5_highest_support = rules[['antecedents','consequents','support']].sort_values('support',ascending=False)[:5]
top5_highest_confidence = rules[['antecedents','consequents','confidence']].sort_values('confidence',ascending=False)[:5]
top5_highest_lift = rules[['antecedents','consequents','lift']].sort_values('lift',ascending=False)[:5]
best_combination = rules[(rules['lift']>=13)&(rules['confidence']>=0.7)].sort_values('lift',ascending=False)

item_list = pd.read_csv("/home/hadoop/final_project/csv_temp/df_item_fact_table.csv").drop(columns=["product_category", "product_sub_category"])
id_to_name = item_list.set_index('id')['item_name'].to_dict()


def re_search(value):
    value = str(value)
    return_value = ''
    for i in range(len(value)):
      try:
        x = int(value[i])
        return_value = return_value + value[i]
      except:
        continue
    return int(return_value)

def frozen_set_remover(df):
  df = df.reset_index().drop(columns="index")
  df = df.astype(str)
  for i in range(len(df)):
    df.loc[i, "antecedents"] = re_search(df.loc[i, "antecedents"])
    df.loc[i, "consequents"] = re_search(df.loc[i, "consequents"])
  return df

def merging(data_raw):
  data = frozen_set_remover(data_raw)
  data['antecedents'] = data['antecedents'].map(id_to_name)
  data['consequents'] = data['consequents'].map(id_to_name)
  numeric_columns = ['antecedent support','consequent support','support','confidence','lift','leverage','conviction','zhangs_metric']  # Add any other numeric columns you need to round
  for col in numeric_columns:
    if col in data.columns:
      data[col] = data[col].astype(float).round(3)
   
  return data


top5_highest_support = merging(top5_highest_support)
top5_highest_confidence = merging(top5_highest_confidence)
top5_highest_lift = merging(top5_highest_lift)
best_combination = merging(best_combination)
rullin = merging(rules)


top5_highest_support.to_csv("/home/hadoop/final_project/csv_output/apriori_output/top5_highest_support.csv")
top5_highest_confidence.to_csv("/home/hadoop/final_project/csv_output/apriori_output/top5_highest_confidence.csv")
top5_highest_lift.to_csv("/home/hadoop/final_project/csv_output/apriori_output/top5_highest_lift.csv")
best_combination.to_csv("/home/hadoop/final_project/csv_output/apriori_output/best_combination.csv")
rullin.to_csv("/home/hadoop/final_project/csv_output/apriori_output/rules.csv")
