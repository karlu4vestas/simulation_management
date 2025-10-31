import os
import random
from datetime import date, timedelta
from collections.abc import Iterable
import bigtree
from bigtree import  list_to_tree

# Type alias for bigtree.Node
FolderTreeNode = bigtree.Node

# Manages a tree structure of folders using the bigtree library,
# allowing for node annotation and decompilation into a table.
class FolderTree:
    def __init__(self, paths: Iterable[str], prefix:str="\\\\", path_separator:str=os.sep):
        # Initializes the FolderTree from a list of path of possibly independent tree.
        # FolderTree orhganizes the paths into a tree structure
        # with an internal root node that does not appear in user-visible paths. 

        # Args:
        #     the paths (list[str]): A list of folder path strings to initialize the trees.
        #     path_separator (str, optional): The path separator to use.
        #                                     Defaults to os.sep.
        self.prefix = prefix
        self.path_separator = path_separator

        # Internal root node for bigtree; its name won't appear in user paths.
        self.internal_root = "ROOT"+self.path_separator

        paths = [self.internal_root + path[len(prefix):] for path in paths]  # Prefix with internal root name
        #print(f"paths for the tree:{'\n'.join(paths)}")        
        self.root= list_to_tree(paths, path_separator, node_type=FolderTreeNode)
        
        #prefix a separator to the root to compensate for bigtree adding the separator
        self.internal_root=self.path_separator+self.internal_root
          
    def findall(self, func):
        nodes = bigtree.findall(self.root, func)
        return nodes

    def get_ascii_tree(self, attr_list:list)-> str:
        return self.root.show(attr_list=attr_list)

    def mark_vts_simulations(self, vts_label:str, vts_names:set[str], vts_hierarchical_label:str, has_vts_children_label:str):
        # Marks folder with:
        # vts_label: does it contain a VTS-simulation. 
        # has_vts_children_label: does it have children that are VTS-simulations.
        # vts_hierarchical_label: does a folder contains a vts simulation AND does it have one or more subfolders with other vts_simulations. 
        #                         This requires "has_vts_children_label" to be propagated to the higher parent etc.
        # At the same time aggregate folder_modified_date to each vts_simulation

        # To produce "vts_hierarchical_label" and "has_vts_children_label" the iteration must go from the leafs to the root
        for node in bigtree.postorder_iter(self.root):
            has_children:bool      = node.get_attr(has_vts_children_label,False)           
            is_vts_simulation:bool = len([c.name for c in node.children if c.name.lower() in vts_names]) == len(vts_names)
            is_hierarchical:bool   = is_vts_simulation and has_children
            
            if is_vts_simulation:
                node.set_attrs({vts_label: node.path_name[len(self.internal_root):]})

            if is_hierarchical:
                node.set_attrs({vts_hierarchical_label: True})

            #propagate to parent
            if node.parent is not None:
                att_parent:bool = node.parent.get_attr(has_vts_children_label,False) #other siblings might already have add to the parent?
                if att_parent or has_children:
                    node.parent.set_attrs({has_vts_children_label: True})
                elif is_vts_simulation: 
                    node.parent.set_attrs({has_vts_children_label: True})




if __name__ == '__main__':
    # @todo: create a test with master component relationsships

    # 2. Add folder paths to create the tree structure
    # folder_paths = [
    #     "usr/local/bin",
    #     "usr/local/lib",
    #     "usr/share",
    #     "home/user/documents",
    #     "home/user/pictures",
    #     "home/guest",
    #     "tmp/cache/data"
    # ]

    # folder_paths = [
    #     "a/eig",
    #     "a/int/",
    #     "a/b",
    #     "a/b/c",
    #     "a/b/c/f1",
    #     "a/b/c/eig",
    #     "a/b/c/int",
    #     "a/b/c/int/d",
    #     "a/b/c/d/e/int",
    #     "a/b/c/d/e/eig",
    #     "a/b/c/d/e/f/g",
    #     "aa/bb/cc",
    #     "aa/bb/cc",
    #     "aa/eig",
    #     "aa/int/",
    #     "aa/bb",
    #     "aa/b/c",
    #     "aa/b/c/f1",
    #     "aa/b/c/eig",
    #     "aa/b/c/int",
    #     "aa/b/c/int/d",
    #     "aa/b/c/d/e/int",
    #     "aa/b/c/d/e/eig",
    #     "aa/b/c/d/e/f/g",
    # ]
    folder_paths = [
        "\\\\a\\eig",
        "\\\\a\\int\\",
        "\\\\a\\b",
        "\\\\a\\b\\c",
        "\\\\a\\b\\c\\f1",
        "\\\\a\\b\\c\\eig",
        "\\\\a\\b\\c\\int",
        "\\\\a\\b\\c\\int\\d",
        "\\\\a\\b\\c\\d\\e",
        "\\\\a\\b\\c\\d\\e\\int",
        "\\\\a\\b\\c\\d\\e\\eig",
        "\\\\a\\b\\c\\d\\e\\f\\g",
        "\\\\aa\\bb\\cc",
        "\\\\aa\\bb\\cc",
        "\\\\aa\\eig",
        "\\\\aa\\int\\",
        "\\\\aa\\bb",
        "\\\\aa\\b\\c",
        "\\\\aa\\b\\c\\f1",
        "\\\\aa\\b\\c\\eig",
        "\\\\aa\\b\\c\\int",
        "\\\\aa\\b\\c\\int\\d",
        "\\\\aa\\b\\c\\d\\e",
        "\\\\aa\\b\\c\\d\\e\\int",
        "\\\\aa\\b\\c\\d\\e\\eig",
        "\\\\aa\\b\\c\\d\\e\\f\\g",
        "\\\\bb\\c\\d",
        "\\\\bb\\c\\d\\f1",
        "\\\\bb\\c\\d\\eig",
        "\\\\bb\\c\\d\\int",
        "\\\\bb\\c\\e",
        "\\\\bb\\c\\e\\f1",
        "\\\\bb\\c\\e\\eig",
        "\\\\bb\\c\\e\\int",
        "\\\\bb\\d\\e",
        "\\\\bb\\d\\e\\f1",
        "\\\\bb\\d\\e\\f2",
        "\\\\bb\\d\\e\\f3",
    ]
    folder_modified_date:dict[str,date] = {f: date.today() - timedelta(days=random.randint(0, 7)) for f in folder_paths}

    paths = ".\n".join(folder_modified_date.keys())
    print(f"specified paths:")
    print(paths)
    prefix="\\\\"
    tree:FolderTree = FolderTree(folder_modified_date.keys(), prefix=prefix, path_separator="\\")
    # n    = tree.find_by_full_path("a/eig")  # Example of finding a path
    # if not n is None:
    #     print("Path 'a/local' found in the tree.")
    #     n.set_attrs( {"sim_node": "eig_int"})
    print(tree.get_ascii_tree(attr_list=["sim_node"]))  # Show the initial tree structure

    # Example of iterating through the tree and printing node names and their children
    vts_label:str              = "vts_simulations"
    vts_hierarchical_label:str = "vts_hierarchical_simulations"
    has_vts_children_label:str = "has_vts_children"
    vts_name_set:set[str]      = set( [name.casefold() for name in ["INPUTS","DETWIND","EIG","INT","LOG", "OUT","PARTS","PROG","STA"] ] )    

    small_vts_name_set:set[str] = set( [name.casefold() for name in ["EIG","INT"] ] )
    tree.mark_vts_simulations(vts_label, small_vts_name_set, vts_hierarchical_label, has_vts_children_label)
    print(tree.get_ascii_tree(attr_list=[vts_label, vts_hierarchical_label, has_vts_children_label]))  # Show the tree and their attributes

    #print a list of all the eig_int folders
    all_simulations = tree.findall(lambda node: len(node.get_attr(vts_label,"")) > 0)
    print(f"Total number of simulations: {len(all_simulations)}")
    print( "\n".join([n.path_name for n in all_simulations]) )
    #print( "\n".join([str(n.get_attr(vts_label,[])) for n in eig_int_parents]) )

    simulations_without_sub_simulations:tuple[FolderTreeNode,...] = tree.findall(lambda node: len(node.get_attr(vts_label,"")) > 0 and not node.get_attr(vts_hierarchical_label,False))
    print(f"\n{len(simulations_without_sub_simulations)} simulations without sub-simulations: ")
    print( "\n".join([n.path_name for n in simulations_without_sub_simulations]) )

    simulations_with_sub_simulations:tuple[FolderTreeNode,...] = tree.findall(lambda node: node.get_attr(vts_hierarchical_label,False))
    print(f"\n{len(simulations_with_sub_simulations)} simulations with sub-simulations: ")
    print( "\n".join([n.path_name for n in simulations_with_sub_simulations]) )

    # for the following to work the folder_modified_date must contain the path of all nodes
    simulations_without_sub_simulations_dict:dict[str,date] = {prefix+n.get_attr(vts_label): folder_modified_date[prefix+n.get_attr(vts_label)] for n in simulations_without_sub_simulations}
    print(f"\n{len(simulations_without_sub_simulations_dict)} simulation dict without sub-simulations: ")
    print( "\n".join([f"{path}: {max_modified_date}" for path, max_modified_date in simulations_without_sub_simulations_dict.items()]) )


    """
    path_col="path"
    p=tree.folder_tree_to_polars(path_col=path_col, attr_dict={sim_type_label:sim_type_label}).select( [path_col,sim_type_label] )
    print(p.columns)
    print(p.head())
    """
    # cannot sa the dataframe becaus it contains list of lists that would have to be converted to strings"
    #p.write_csv("C:\\Users\\karlu\\Downloads\\folder_tree.csv")