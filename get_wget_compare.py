import urllib.request
import os
import json
import datetime
import argparse

def get_datasets(data_node, index_node, num_datasets):
    search_url = 'https://esgf-node.llnl.gov/esg-search/search/' \
                 '?limit=0&format=application%2Fsolr%2Bjson'

    with urllib.request.urlopen(search_url) as url:
        results = json.loads(url.read().decode('UTF-8'))
    shards = results['responseHeader']['params']['shards']
    
    query_url = 'https://esgf-node.llnl.gov/solr/datasets/select' \
               '?q=*:*&fl=id&wt=json&facet=true&fq=type:Dataset' \
               '&fq=replica:false&fq=latest:true&fq=project:CMIP6' \
               '&rows={rows}&shards={shards}' \
               '&fq=data_node:{data_node}&fq=index_node:{index_node}'
    query = query_url.format(rows=num_datasets, 
                             shards=shards, 
                             data_node=data_node, 
                             index_node=index_node) 
    
    with urllib.request.urlopen(query) as url:  
        results = json.loads(url.read().decode('UTF-8'))
    return results['response']['docs']

def gen_script(data_node, index_node, num_datasets, file_limit, output_dir):

    dataset_list = get_datasets(data_node, index_node, num_datasets)
    
    esgf_wget_api_url = 'https://{index_node}/esg-search/wget/?' \
                        'distrib=false&limit={limit}&{query}'

    local_wget_api_url = 'http://127.0.0.1:8000/wget?limit={limit}&{query}'

    dataset_query = 'dataset_id={}'.format('&dataset_id='.join([d['id'] for d in dataset_list]))

    urllib.request.urlretrieve(esgf_wget_api_url.format(index_node=index_node, 
                                                        limit=file_limit, 
                                                        query=dataset_query), 
                               os.path.join(output_dir, 'wget-esgf-node.sh'))
    
    urllib.request.urlretrieve(local_wget_api_url.format(limit=file_limit, 
                                                         query=dataset_query), 
                               os.path.join(output_dir, 'wget-{}.sh'.format(index_node)))

def main():

    parser = argparse.ArgumentParser(description='Get wget scripts from both an index node ' \
                                                 'and a locally-run wget API using the CMIP6 datasets')
    parser.add_argument('--data_node', '-d', dest='data_node', 
                        type=str, help='Data Node', required=True)
    parser.add_argument('--index_node', '-i', dest='index_node', 
                        type=str, default='esgf-node.llnl.gov', help='Index Node')
    parser.add_argument('--num_datasets', '-nd', dest='num_datasets', 
                        type=int, default=10, help='Number of Datasets')
    parser.add_argument('--file_limit', '-fl', dest='file_limit', 
                        type=int, default=1000, help='File number limit for wget API')
    parser.add_argument('--output', '-o', dest='output', type=str, default=os.path.curdir, 
                        help='Output directory (default is current directory)')
    args = parser.parse_args()

    if not os.path.isdir(args.output):
        print('{} is not a directory. Exiting.'.format(args.output))
        return

    gen_script(args.data_node, args.index_node, 
               args.num_datasets, args.file_limit, args.output)


if __name__ == '__main__':
	main()

