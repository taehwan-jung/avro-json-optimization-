import pandas as pd
import numpy as np

# random data generating
np.random.seed(43)
n_rows = 5000000
df = pd.DataFrame({
    'user_id' : np.random.randint(1,100000,n_rows),
    'action_type' : np.random.choice(['view', 'click', 'like', 'share'], n_rows),
    'blog_id': np.random.randint(1, 50000, n_rows),
    'timestamp': pd.date_range('2026-01-01', periods =n_rows, freq='s')
    
})

# downcasting
def get_optimized_df():
    df_optimized = df.copy()
    df_optimized['user_id'] = df_optimized['user_id'].astype(np.uint32)
    df_optimized['action_type'] = df_optimized['action_type'].astype('category').cat.codes.astype(np.uint8)
    df_optimized['blog_id'] = df_optimized['blog_id'].astype(np.uint16)
    df_optimized['timestamp'] = pd.to_datetime(df_optimized['timestamp'], format = '%Y-%m-%d')
    return df_optimized


# optimized_df
df_final = get_optimized_df()

# main()
if __name__ == "__main__":

# before downcasting
    print(f"최적화 전 메모리: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    print(df.head(5))
    print(df.max())
    print(df.min())
    print(df.dtypes)

# after downcasting
    print(f"최적 화 후 메모리: {df_optimized.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    print(df_optimized.head(5))
    print(df_optimized.max())
    print(df_optimized.min())
    print(df_optimized.dtypes)
