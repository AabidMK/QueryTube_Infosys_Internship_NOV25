import pandas as pd
import os

def merge_youtube_transcripts():
    print("📂 Checking available files...")
    
    # List all CSV files
    csv_files = [f for f in os.listdir() if f.endswith('.csv')]
    print(f"Found CSV files: {csv_files}")
    
    # Check for your specific files
    file1 = None
    file2 = None
    
    # Look for monk&warriors file (might have different exact name)
    for file in csv_files:
        if 'monk' in file.lower() or 'warrior' in file.lower():
            file1 = file
        elif 'transcript' in file.lower():
            file2 = file
    
    if not file1 or not file2:
        print("❌ Could not find both required files!")
        print("Please make sure you have:")
        print("1. Your original CSV file (monk&warriors or similar)")
        print("2. youtube_videos_with_transcripts.csv")
        return None
    
    print(f"\n📄 File 1 (original): {file1}")
    print(f"📄 File 2 (with transcripts): {file2}")
    
    # Step 1: Load both CSV files
    print("\n⏳ Loading files...")
    
    try:
        df_original = pd.read_csv(file1)
        print(f"✅ Loaded {file1}: {len(df_original)} rows, {len(df_original.columns)} columns")
        
        df_transcripts = pd.read_csv(file2)
        print(f"✅ Loaded {file2}: {len(df_transcripts)} rows, {len(df_transcripts.columns)} columns")
    except Exception as e:
        print(f"❌ Error loading files: {e}")
        return None
    
    # Step 2: Display column names for verification
    print("\n📊 COLUMN ANALYSIS:")
    print(f"File 1 columns: {list(df_original.columns)}")
    print(f"File 2 columns: {list(df_transcripts.columns)}")
    
    # Step 3: Check for common columns (video ID)
    id_column = None
    for col in ['id', 'video_id', 'videoId', 'ID']:
        if col in df_original.columns and col in df_transcripts.columns:
            id_column = col
            break
    
    if not id_column:
        print("❌ Could not find common ID column!")
        print("Looking for columns: id, video_id, videoId, ID")
        return None
    
    print(f"\n✅ Common ID column found: '{id_column}'")
    
    # Step 4: Merge transcripts
    print("\n🔗 Merging transcripts...")
    
    # Check if transcript column exists in second file
    transcript_column = None
    for col in ['transcript', 'Transcript', 'TRANSCRIPT']:
        if col in df_transcripts.columns:
            transcript_column = col
            break
    
    if not transcript_column:
        print("❌ Could not find transcript column in second file!")
        return None
    
    print(f"✅ Transcript column: '{transcript_column}'")
    
    # Create a dictionary for mapping
    transcript_dict = dict(zip(df_transcripts[id_column], df_transcripts[transcript_column]))
    
    # Add transcript column to original dataframe
    df_original['transcript'] = df_original[id_column].map(transcript_dict)
    
    # Step 5: Statistics
    transcripts_added = df_original['transcript'].notna().sum()
    print(f"✅ Transcripts successfully added to {transcripts_added} out of {len(df_original)} videos")
    
    # Step 6: Save merged file
    output_filename = 'youtube_channel_data_final.csv'
    df_original.to_csv(output_filename, index=False, encoding='utf-8-sig')
    
    print(f"\n💾 MERGED FILE SAVED: {output_filename}")
    print(f"   Total videos: {len(df_original)}")
    print(f"   Videos with transcripts: {transcripts_added}")
    print(f"   Videos without transcripts: {len(df_original) - transcripts_added}")
    
    # Step 7: Show sample
    print("\n🔍 SAMPLE DATA (first video):")
    if len(df_original) > 0:
        first_row = df_original.iloc[0]
        print(f"   ID: {first_row[id_column]}")
        print(f"   Title: {first_row.get('title', 'N/A')[:50]}...")
        
        transcript = first_row.get('transcript', '')
        if pd.isna(transcript):
            print("   Transcript: NOT AVAILABLE")
        else:
            transcript_str = str(transcript)
            print(f"   Transcript (first 100 chars): {transcript_str[:100]}...")
    
    return df_original

def simple_merge():
    """Simpler version if you know exact file names"""
    print("\n🔄 TRYING SIMPLE MERGE...")
    
    # Try different possible file names
    possible_files = ['monk&warriors.csv', 'monk_and_warriors.csv', 'youtube_data.csv']
    
    for file in possible_files:
        if os.path.exists(file):
            print(f"Found: {file}")
            
            df1 = pd.read_csv(file)
            df2 = pd.read_csv('youtube_videos_with_transcripts.csv')
            
            # Simple merge
            df1['transcript'] = df2['transcript']
            
            output_file = 'merged_youtube_data.csv'
            df1.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            print(f"✅ Simple merge complete! Saved as {output_file}")
            print(f"   Total videos: {len(df1)}")
            print(f"   Transcripts added: {df1['transcript'].notna().sum()}")
            
            return df1
    
    print("❌ Could not find original CSV file")
    return None

# Main execution
if __name__ == "__main__":
    print("=" * 60)
    print("YOUTUBE DATA MERGER - INFOSYS SPRINGBOARD")
    print("=" * 60)
    
    print("\nYour current directory:", os.getcwd())
    print("\nAvailable CSV files:")
    for file in os.listdir():
        if file.endswith('.csv'):
            print(f"  • {file}")
    
    print("\nChoose merge method:")
    print("1. Smart Merge (Recommended - automatically finds files)")
    print("2. Simple Merge (If you know exact file names)")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    if choice == "1":
        print("\n" + "="*40)
        print("RUNNING SMART MERGE...")
        print("="*40)
        merge_youtube_transcripts()
    
    elif choice == "2":
        print("\n" + "="*40)
        print("RUNNING SIMPLE MERGE...")
        print("="*40)
        simple_merge()
    
    else:
        print("❌ Invalid choice. Running smart merge...")
        merge_youtube_transcripts()
    
    print("\n" + "="*60)
    print("PROCESS COMPLETE!")
    print("="*60)
    print("\n📁 Check your folder for the merged CSV file!")