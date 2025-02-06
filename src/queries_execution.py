from queries_db_script import query_1, query_2, query_3, query_4, query_5

def main():
    """
    Main demonstration function calling each query with sample inputs
    and printing the results. Explanations are included for sorting/limiting.
    """

    # -------------------------------------------------------------------------
    # QUERY #1 Demo
    # -------------------------------------------------------------------------
    print("\n=== Query #1 Demo ===")
    print("(Find the single most popular movie, then return the top 10 actors for it.)")
    print("Sorting Explanation: In the subquery, we pick the highest-popularity movie;\n"
          "then we join cast members and sort them by p.popularity DESC, limiting to 10.\n")
    results_1 = query_1()
    print(f"Query #1 returned {len(results_1)} rows:")
    for row in results_1:
        print(row)

    # -------------------------------------------------------------------------
    # QUERY #2 Demo
    # -------------------------------------------------------------------------
    print("\n=== Query #2 Demo ===")
    print("(For each genre, get the total # of movies and the single most popular movie.)")
    print("Sorting Explanation: We group by each genre ID, then 'ORDER BY movie_count DESC',\n"
          "so the genre with the most movies appears first.\n")
    results_2 = query_2()
    print(f"Query #2 returned {len(results_2)} rows:")
    for row in results_2[:5]:  # Show only the first 5 for brevity
        print(row)

    # -------------------------------------------------------------------------
    # QUERY #3 Demo
    # -------------------------------------------------------------------------
    print("\n=== Query #3 Demo ===")
    print("(Full-text search in movies.title for substring, using Boolean mode.)")
    print("Sorting Explanation: We 'ORDER BY popularity DESC', returning up to 10 matches.\n")
    substring_3 = "man"  # Example substring
    results_3 = query_3(substring_3)
    print(f"Query #3 (search='{substring_3}') returned {len(results_3)} rows:")
    for row in results_3[:10]:  # Show only first 10
        print(row)

    # -------------------------------------------------------------------------
    # QUERY #4 Demo
    # -------------------------------------------------------------------------
    print("\n=== Query #4 Demo ===")
    print("(Full-text search in movie_credits.character_name_or_job_title using Boolean mode.)")
    print("We retrieve DISTINCT movie IDs/titles that match the substring, limited to 100.\n"
          "Note: We do not explicitly sort the results in this query.\n")
    substring_4 = "Robin"  # Example substring
    results_4 = query_4(substring_4)
    print(f"Query #4 (search='{substring_4}') returned {len(results_4)} rows:")
    for row in results_4[:10]:  # Show only first 10
        print(row)

    # -------------------------------------------------------------------------
    # QUERY #5 Demo
    # -------------------------------------------------------------------------
    print("\n=== Query #5 Demo ===")
    print("(Lists all movies that have at least one cast member with popularity > X.)")
    print("Sorting Explanation: No explicit sorting, we only check if such a cast member EXISTS.\n")
    min_popularity_example = 10
    results_5 = query_5(min_popularity_example)
    print(f"Query #5 (popularity > {min_popularity_example}) returned {len(results_5)} rows:")
    for row in results_5[:10]:  # Show only the first 10
        print(row)

    print("\n=== End of Demo ===")

if __name__ == "__main__":
    main()