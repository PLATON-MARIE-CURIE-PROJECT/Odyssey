import numpy
import matplotlib.pyplot as plt
import array


def plot_data(x,y, filepath, title, color, labelX, labelY):
    plt.scatter(x,y, facecolors='none', edgecolors=color)
    plt.title(title)
    plt.xlabel(labelX)
    plt.ylabel(labelY)
    plt.savefig(filepath)
    plt.close()
      
queries = 10
subtrees = 10
filename = "/gpfs/users/chatzakis/queries_statistics/subtree_stats_per_query.txt"

data = []
debug_line = ""

with open(filename) as file:
    line_counter = 0
    current_query = 0
    current_subtree = 0
    prev_query = 0
    for line in file:
        
        if line_counter == 0:
            line_counter += 1
            continue #first line contains info, we skip
        
        raw = line.rstrip().split()
        line_vals = [float(x) for x in raw]

        if line_counter == 1:
            queries = int(line_vals[0])
            subtrees = int(line_vals[1])
            #data = numpy.zeros((queries, subtrees))
            data = [[0 for x in range(subtrees)] for y in range(queries)] 
            line_counter += 1
            continue

        if line_counter == 2:
            debug_line = line

        current_query = int(line_vals[0])
        current_subtree = int(line_vals[1])
        #print(current_query, current_subtree)
        data[current_query][current_subtree] = line_vals[2:]

        line_counter += 1


height_list = []
unpruned_nodes_list = []
total_nodes_list = []
dist_list = []
min_dists_list = []
tree_bsf_list = []

for query in range(queries):
    for subtree in range(subtrees):
        
        height =  data[query][subtree][0]
        total_nodes = data[query][subtree][1]
        dist = data[query][subtree][2]
        min_dists = data[query][subtree][3]
        unpruned_nodes = data[query][subtree][4]
        tree_bsf = data[query][subtree][5]
        
        if height>0:
            height_list.append(height)
            unpruned_nodes_list.append(unpruned_nodes)
            total_nodes_list.append(total_nodes)
            dist_list.append(dist)
            min_dists_list.append(min_dists)
            tree_bsf_list.append(tree_bsf)

        #print(query, subtree, height, total_nodes, dist, min_dists, unpruned_nodes)

#print("Total subtrees analyzed: ", queries*subtrees)

#plot_data(unpruned_nodes_list, height_list, "./plots/height_vs_unpruned_leafs.png", "Unpruned nodes vs Height", "blue", "Unpruned Nodes", "Height")
#plot_data(min_dists_list, height_list,      "./plots/height_vs_mindists.png",      "Min Dists vs Height",      "blue", "Min Dists",      "Height")

#plot_data(unpruned_nodes_list, total_nodes_list, "./plots/total_nodes_vs_unpruned_leafs.png", "Unpruned nodes vs Total Nodes", "blue", "Unpruned Nodes", "Total Nodes")
#plot_data(min_dists_list, total_nodes_list,      "./plots/total_nodes_vs_mindists.png",       "Min Dists vs Total Nodes",      "blue", "Min Dists",      "Total Nodes")

#plot_data(unpruned_nodes_list, dist_list, "./plots/distance_from_root_vs_unpruned_leafs.png", "Unpruned nodes vs Distance From Root", "blue", "Unpruned Nodes", "Distance From Root")
#plot_data(min_dists_list, dist_list,      "./plots/distance_from_root_vs_mindists.png",       "Min Dists vs Distance From Root",      "blue", "Min Dists",      "Distance From Root")

#plot_data(unpruned_nodes_list, tree_bsf_list, "./plots/tree_bsf_vs_unpruned_leafs.png", "Unpruned nodes vs Tree BSF", "blue", "Unpruned Nodes", "Tree BSF")
#plot_data(min_dists_list, tree_bsf_list,      "./plots/tree_bsf_vs_mindists.png",       "Min Dists vs Tree BSF",      "blue", "Min Dists",      "Tree BSF")

sum = 0
for subtree in range(subtrees):
    
    if data[0][subtree][0] == 0:
        sum += 1

print("perc of empty subtrees is: ", (sum*1.0/subtrees)*100, "%")


print("Parsed Data [0][0]", data[0][0])
print("Raw Data", debug_line)
  
