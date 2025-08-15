
namespace VSM.Client.Datamodel
{
        /*
        public class RootFolder
        {
                private InnerNode? _folderTree;
                private Task<InnerNode?>? _folderTreeTask;
                public int Id { get; set; }                
                public bool Is_registeredfor_cleanup { get; set; } = false;
                public string Root_path { get; set; } = "";
                
                public string Owner { get; set; } = "";
                
                public string Approvers { get; set; } = "";

                public FolderNode? FolderTree
                {
                        get
                        {
                                if (_folderTree == null && _folderTreeTask?.IsCompleted == true)
                                {
                                        _folderTree = _folderTreeTask.Result;
                                }
                                return _folderTree;
                        }
                        set => _folderTree = value;
                }

                // Async method to get folder tree
                public async Task<FolderNode?> GetFolderTreeAsync()
                {
                        if (_folderTree != null)
                                return _folderTree;

                        if (_folderTreeTask == null)
                        {
                                _folderTreeTask = DataModel.Instance.GetFolderTreeAsync(this);
                        }

                        _folderTree = await _folderTreeTask;
                        return _folderTree;
                }

                // Check if tree generation is in progress
                public bool IsLoadingFolderTree => _folderTreeTask != null && !_folderTreeTask.IsCompleted;

                public List<User> Users { get; set; } = [];
        }

        //  ------------------------------ Test data generation ------------------------------
        public static class TestDataGenerator
        {
                class RandomRetention
                {
                        private Random rand_int_generator;
                        private List<RetentionType> retentiontypes;
                        
                        public RandomRetention(int seed, List<RetentionType> retentionTypes)
                        {
                                rand_int_generator = new Random(seed);
                                retentiontypes = retentionTypes;
                        }
                        
                        public RetentionType Next()
                        {
                                return retentiontypes[rand_int_generator.Next(0, retentiontypes.Count)];
                        }
                }
                public class BooleanGenerator
                {
                        Random rnd;

                        public BooleanGenerator()
                        {
                                rnd = new Random();
                        }

                        public bool NextBoolean()
                        {
                                return rnd.Next(0, 2) == 1;
                        }
                }


                //return the folder hierarchy that match the path to the rootfolder.
                public static async Task<FolderNode?> GetRootFolderTreeAsync(RootFolder root_folder)
                {
                        if (root_folder == null)
                                return null;

                        var retentionTypes = await DataModel.Instance.GetRetentionOptionsAsync();
                        RandomRetention retention_generator = new RandomRetention(0, retentionTypes);
                        int idCounter = 1;
                        root_folder.FolderTree = new InnerNode()
                        {
                                Id = idCounter,
                                ParentId = idCounter,
                                Name = root_folder.Root_path,
                                IsExpanded = true,
                                Level = 0,
                        };

                        idCounter = root_folder.FolderTree.Id;


                        // Generate n_levels=10 levels in the folder tree with InnerNode (can have children) and LeafNode (no children)
                        // generate up to n_children=4 at each level
                        // at any level chose randomly between generating an InnerNodes or a LeafNode 
                        Random random = new Random(42); // Use fixed seed for reproducible results
                        await GenerateTreeRecursivelyAsync(root_folder.FolderTree, idCounter, retention_generator, new BooleanGenerator(), random, maxLevel: 12);
                        return root_folder.FolderTree;
                }

                private static async Task GenerateTreeRecursivelyAsync(InnerNode parent, int idCounter, RandomRetention retentionGenerator, BooleanGenerator boolGen, Random random, int maxLevel)
                {
                        Console.WriteLine($"Start GenerateTreeRecursivelyAsync: maxLevel = {maxLevel}");

                        // Simple level-by-level processing using a queue
                        var currentLevelNodes = new List<InnerNode> { parent };
                        int nodesGenerated = 0;
                        const int YIELD_EVERY_N_NODES = 100; // Yield every 100 nodes

                        for (int level = 0; level < maxLevel; level++)
                        {
                                var nextLevelNodes = new List<InnerNode>();

                                foreach (var currentParent in currentLevelNodes)
                                {
                                        // Randomly decide how many children this node should have (5-9)
                                        int numberOfChildren = random.Next(4, 7); // Adjusted to 2-5 for more variability

                                        for (int i = 0; i < numberOfChildren; i++)
                                        {
                                                var childId = ++idCounter;
                                                var childLevel = level + 1;

                                                TreeNode child;

                                                if (childLevel == maxLevel)
                                                {
                                                        // At the final level, create only LeafNodes
                                                        child = new LeafNode
                                                        {
                                                                Id = childId,
                                                                ParentId = currentParent.Id,
                                                                Parent = currentParent,
                                                                Name = $"SimData_{childLevel}_{i + 1}",
                                                                Level = childLevel,
                                                                Retention = retentionGenerator.Next().name
                                                        };
                                                }
                                                else
                                                {
                                                        // Before the final level, randomly choose between InnerNode and LeafNode
                                                        bool shouldBeLeafNode = childLevel > 3 ? boolGen.NextBoolean() : false;

                                                        if (shouldBeLeafNode)
                                                        {
                                                                child = new LeafNode
                                                                {
                                                                        Id = childId,
                                                                        ParentId = currentParent.Id,
                                                                        Parent = currentParent,
                                                                        Name = $"SimData_{childLevel}_{i + 1}",
                                                                        Level = childLevel,
                                                                        Retention = retentionGenerator.Next().name
                                                                };
                                                        }
                                                        else
                                                        {
                                                                child = new InnerNode
                                                                {
                                                                        Id = childId,
                                                                        ParentId = currentParent.Id,
                                                                        Parent = currentParent,
                                                                        Name = $"Folder_{childLevel}_{i + 1}",
                                                                        Level = childLevel
                                                                };

                                                                // Add InnerNode to next level for processing
                                                                nextLevelNodes.Add((InnerNode)child);
                                                        }
                                                }

                                                currentParent.Children.Add(child);
                                                nodesGenerated++;

                                                // Yield control every N nodes to prevent UI blocking
                                                if (nodesGenerated % YIELD_EVERY_N_NODES == 0)
                                                {
                                                        Console.WriteLine($"GenerateTreeRecursivelyAsync: YIELD_EVERY_N_NODES : Total nodes generated = {nodesGenerated}");
                                                        await Task.Yield();
                                                }
                                        }
                                }

                                // Move to next level
                                currentLevelNodes = nextLevelNodes;

                                // If no more InnerNodes to process, we're done
                                if (currentLevelNodes.Count == 0)
                                        break;
                        }

                        Console.WriteLine($"GenerateTreeRecursivelyAsync: Total nodes generated = {nodesGenerated}");
                }

                public static List<RootFolder> GenTestRootFoldersForUser(User user)
                {
                        List<RootFolder> rootFolders = new List<RootFolder>();
                        int rootId = 1;
                        rootFolders.Add(
                                        new RootFolder
                                        {
                                                Id = rootId,
                                                Is_registeredfor_cleanup = true,
                                                Users = [user, new User("jajac"), new User("misve")],
                                                Root_path = "\\\\domain.net\\root_1"
                                        });

                        rootFolders.Add(
                                        new RootFolder
                                        {
                                                Id = ++rootId,
                                                Is_registeredfor_cleanup = true,
                                                Users = [user, new User("stefw"), new User("misve")],
                                                Root_path = "\\\\domain.net\\root_2"
                                        });

                        rootFolders.Add(
                                        new RootFolder
                                        {
                                                Id = rootId,
                                                Users = [user, new User("facap"), new User("misve")],
                                                Root_path = "\\\\domain.net\\root_3"

                                        });

                        rootFolders.Add(
                                        new RootFolder
                                        {
                                                Id = ++rootId,
                                                Users = [user, new User("caemh"), new User("arlem")],
                                                Root_path = "\\\\domain.net\\root_4"
                                        });

                        return rootFolders;
                }
        }
        */

}