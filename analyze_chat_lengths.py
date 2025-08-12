import pandas as pd
from query import *


def parse_message_counts(df):
    """Get message counts from dataframe"""
    df = df.copy()
    
    def get_message_count(row):
        # use msg_count if not null
        if 'msg_count' in row and row['msg_count'] is not None:
            return row['msg_count'].get('msg_count', 0)

        if 'messages' in row and row['messages'] is not None:
            return len(row['messages'])

        return 0
    
    df['message_count'] = df.apply(get_message_count, axis=1)
    return df

def get_simple_stats(df):
    """get simple stats for chat lengths"""
    
    # basic
    stats = {
        'total_chats': len(df),
        'chats_with_messages': len(df[df['message_count'] > 0]),
        'empty_chats': len(df[df['message_count'] == 0]),
        'avg_message_count': df['message_count'].mean(),
        'avg_non_empty_chats': df[df['message_count'] > 0]['message_count'].mean(),
        'median_messages': df['message_count'].median(),
        'std_deviation': df['message_count'].std(),
        'min_messages': df['message_count'].min(),
        'max_messages': df['message_count'].max(),
    }
    
    # percentiles
    percentiles = [25, 50, 75, 90, 95]
    for p in percentiles:
        stats[f'{p}th_percentile'] = int(df['message_count'].quantile(p/100))
    
    # distribution categories
    stats['empty_chats'] = len(df[df['message_count'] == 0])
    stats['very_short_chats'] = len(df[df['message_count'].between(1, 2)])
    stats['short_chats'] = len(df[df['message_count'].between(3, 5)])
    stats['medium_chats'] = len(df[df['message_count'].between(6, 10)])
    stats['long_chats'] = len(df[df['message_count'].between(11, 20)])
    stats['very_long_chats'] = len(df[df['message_count'] > 20])
    
    # percentages
    total = len(df)
    stats['empty_percentage'] = (stats['empty_chats'] / total) * 100
    stats['short_percentage'] = (stats['short_chats'] / total) * 100
    stats['medium_percentage'] = (stats['medium_chats'] / total) * 100
    stats['long_percentage'] = (stats['long_chats'] / total) * 100
    
    return stats

def get_stats_by_type(df):
    """Get stats by chat type (e.g. question, wiki, chat, flashcards)"""
    if 'chat_type' not in df.columns:
        return None
    
    type_analysis = {}
    
    for chat_type in df['chat_type'].unique():
        if pd.isna(chat_type):
            continue
            
        subset = df[df['chat_type'] == chat_type]
        type_analysis[chat_type] = {
            'count': len(subset),
            'avg_length': subset['message_count'].mean(),
            'median_length': subset['message_count'].median(),
            'empty_chats': len(subset[subset['message_count'] == 0]),
            'long_chats': len(subset[subset['message_count'] > 15])
        }
    
    return pd.DataFrame(type_analysis)

def get_full_distribution(df):
    """Get distribution of chat lengths"""
    
    distribution = df['message_count'].value_counts().sort_index()
    
    dist_data = []
    total = len(df)
    
    for length, count in distribution.items():
        dist_data.append({
            'message_count': int(length),
            'frequency': int(count),
            'percentage': round((count / total) * 100, 2),
            'cumulative_percentage': round((distribution[:length+1].sum() / total) * 100, 2)
        })
    
    return pd.DataFrame(dist_data)

def summarize(df):
    """Print nice summary"""
    print("=" * 50)
    print("CHAT LENGTH ANALYSIS")
    print("=" * 50)
    
    stats = get_simple_stats(df)
    print(f"\nCORE STATS:")
    print(f"Total chats: {stats['total_chats']:,}")
    print(f"Chats with messages: {stats['chats_with_messages']:,} ({(stats['chats_with_messages']/stats['total_chats']*100):.1f}%)")
    print(f"Empty chats: {stats['empty_chats']:,} ({stats['empty_percentage']:.1f}%)")
    print(f"Average messages per chat: {stats['avg_message_count']:.2f}")
    print(f"Average for non-empty chats: {stats['avg_non_empty_chats']:.2f}")
    print(f"Median messages: {stats['median_messages']:.1f}")
    print(f"Range: {stats['min_messages']:.0f} - {stats['max_messages']:.0f}")
    print(f"Standard deviation: {stats['std_deviation']:.2f}")


    
    print(f"\nDISTRIBUTION:")
    print(f"Empty: {stats['empty_chats']:,} chats")
    print(f"Very short (1-2): {stats['very_short_chats']:,} chats")
    print(f"Short (3-5): {stats['short_chats']:,} chats")
    print(f"Medium (6-10): {stats['medium_chats']:,} chats")
    print(f"Long (11-20): {stats['long_chats']:,} chats")
    print(f"Very long (20+): {stats['very_long_chats']:,} chats")
    
    print(f"\nPERCENTILES:")
    for p in [25, 50, 75, 90, 95]:
        print(f"{p}th: {stats[f'{p}th_percentile']} messages")
    
    print(f"\nBY CHAT TYPE:")
    type_analysis = get_stats_by_type(df)
    if type_analysis is not None:
        print(type_analysis.T.round(2))
    
    return stats

def analyze(sample_size=1000, include_empty_chats=True):
    """Run analysis on chat lengths"""

    print("Fetching chat data...")
    df = get_chats(sample_size, include_empty_chats) 
    print(f"Retrieved {len(df)} total chats")
    print(f"Include empty chats: {include_empty_chats}")
    
    df = parse_message_counts(df)
    stats = summarize(df)
    
    distribution = get_full_distribution(df)
    
    print("\nSaving results...")
    save_json(pd.DataFrame([stats]), "chat_length_stats.json")
    save_json(distribution, "chat_lengths_full_distributions.json")

analyze(include_empty_chats=False)