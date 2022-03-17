using System;
using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Indexes.Models;
using AzureSearch.BulkInsert;
using System.Net.Http;
using System.Threading.Tasks;
using ServiceStack;
using System.Collections.Generic;

namespace ConsoleApp1
{
    class Program
    {
        const string BOOKS_URL = "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/books.csv";
        const string booknewurl= @"D:\azure-search\search-website\bulk-insert\books.csv";

        const string SEARCH_ENDPOINT = "https://ngenpocazcog02.search.windows.net";
        const string SEARCH_KEY = "D41E7ECB602B85461ED4B0834F9E399B";
        const string SEARCH_INDEX_NAME = "good-books-demo";

        static void Main(string[] args)
        {
            Uri searchEndpointUri = new Uri(SEARCH_ENDPOINT);

            SearchClient client = new SearchClient(
                searchEndpointUri,
                SEARCH_INDEX_NAME,
                new AzureKeyCredential(SEARCH_KEY));

            SearchIndexClient clientIndex = new SearchIndexClient(
                searchEndpointUri,
                new AzureKeyCredential(SEARCH_KEY));

           // CreateIndexAsync(clientIndex).Wait();
            BulkInsertAsync(client).Wait();
        }
        static async Task CreateIndexAsync(SearchIndexClient clientIndex)
        {
            Console.WriteLine("Creating (or updating) search index");
            SearchIndex index = new BookSearchIndex(SEARCH_INDEX_NAME);
            var result = await clientIndex.CreateOrUpdateIndexAsync(index);

            Console.WriteLine(result);
        }

        static async Task BulkInsertAsync(SearchClient client)
        {
            Console.WriteLine("Download data file");
            using HttpClient httpClient = new HttpClient();
            //var csv = await httpClient.GetStringAsync(BOOKS_URL);
            var csv =  System.IO.File.ReadAllText(booknewurl);

            Console.WriteLine("Reading and parsing raw CSV data");
            var books =
                csv.ReplaceFirst("book_id", "id").FromCsv<List<BookModel>>();

            Console.WriteLine("Uploading bulk book data");
            var result = await client.UploadDocumentsAsync(books);

            Console.WriteLine("Finished bulk inserting book data");
        }
    }
}
