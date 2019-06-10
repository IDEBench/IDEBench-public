from collections import defaultdict
from collections import deque
from collections import OrderedDict
from common.viz import Viz
import logging
logger = logging.getLogger("idebench")

class VizGraph(object):

    def __init__(self):
        self._graph = OrderedDict()
        self.nodes = set()
    
    def add_viz(self, data):
        if data["name"] not in self.get_nodes_dict():
            viz = Viz.createFromDict(data)
            self.nodes.add(viz)

    def apply_interaction(self, operation):
        # initialize a set of vizs that are affected by this operation
        vizs_to_request = OrderedDict()
        
        if operation.get_viz_name() not in self.get_nodes_dict():
            viz = Viz.createFromDict(operation.data)
            self.nodes.add(viz)
            vizs_to_request[viz] = True

        viz_dict = self.get_nodes_dict()
        current_viz = viz_dict[operation.get_viz_name()]

        if operation.has_filter():
            current_viz.filter = operation.get_filter()
            #vizs_to_request.add(current_viz)
            vizs_to_request[current_viz] = True

        # parse source attribute
        if operation.has_source():
            source = operation.get_source()
            if len(source) > 0:
               
                # find all current sources add check which ones have been added/removed
                old_sources = current_viz.get_source_vizs()
                new_sources = operation.get_source_vizs()
                sources_removed = old_sources - new_sources
               
                #sources_remained = new_sources.intersection(old_sources)
                sources_added = new_sources - sources_removed

                for src in sources_removed:
                    self.remove_connection(viz_dict[src], current_viz)
                    #if "remove_link" in self.interface.remove_link:
                    #    self.interface.remove_link(current_viz, viz_dict[src])

                for src in sources_added:
                    self.add_connection(viz_dict[src], current_viz)
                    #add_link_method = getattr(self.interface, "add_link", None)
                    #if callable(add_link_method):
                    #    self.interface.add_link(current_viz, viz_dict[src])
                    #    return

            # update the source of the current viz
            current_viz.source = operation.get_source()
            #vizs_to_request.add(current_viz)
            vizs_to_request[current_viz] = True
            #self.process_next_interaction()
            return vizs_to_request

        # parse selection
        if operation.has_selection():
            current_viz.selection = operation.get_selection()
            # find other vizs affected by this selection
            affected_vizs = self.update_affected_vizs(current_viz, viz_dict)
            
            # dont update viz where selection was made
            if current_viz in affected_vizs:
                del affected_vizs[current_viz]

            vizs_to_request.update()
            
        current_viz.set_computed_filter(self.compute_filter(current_viz, viz_dict))
        
        
        # set the parent id of each viz request
        #for viz_to_request in vizs_to_request.keys():
        #   self.parent_operations[viz_to_request] = index

        return affected_vizs.keys()
        #self.viz_requests = []
        #for viz in vizs_to_request.keys():
        #    self.operation_count += 1
        #    self.viz_requests.append(VizRequest(self.operation_count, index, viz))
#
 #       self.interface.request_vizs(self.viz_requests)

    def update_affected_vizs(self, current_viz, viz_dict):
        dependent_vizs = self.find_dependencies_top_down(current_viz)
        vizs_to_request = OrderedDict()
        for viz in dependent_vizs:
            computed_filter = self.compute_filter(viz, viz_dict)
            viz.set_computed_filter(computed_filter)
            vizs_to_request[viz] = True
        return vizs_to_request

    def compute_filter(self, viz, viz_dict):
        computed_vizs = set()
        def compute_filter_inner(start, selections, filter_strs, source_strs):
            computed_vizs.add(start)

            if start.has_filter():
                filter_strs.append(start.filter)

            if start.selection and not start.selection == "":
                selections[start.name] = start.selection

            if not start.source or start.source == "":
                return

            source_strs.extend(start.get_source_vizs())
            sources = start.get_source_vizs()

            for src in sources:
                if viz_dict[src] not in computed_vizs:
                    compute_filter_inner(viz_dict[src], selections, filter_strs, source_strs)

        source_strs_list = []
        selections = {}
        filters = []
        compute_filter_inner(viz, selections, filters, source_strs_list)
        source_strs = " and ".join(list(set(source_strs_list)))
        for src in list(set(source_strs_list)):
            if src in selections:                
                source_strs = source_strs.replace(src, selections[src])
            else:
                source_strs = source_strs.replace(src, "NULL")
        source_strs = source_strs.replace("and NULL", "").replace("NULL and", "").replace("or NULL", "").replace("NULL or", "").replace("NULL", "").strip().lstrip("and ")
        if len(source_strs) > 0 and len(filters) > 0:
            source_strs += " AND "
        return source_strs + " AND ".join(filters)

    def get_nodes(self):
        return self.nodes

    def get_nodes_dict(self):
        d = {}
        for node in self.get_nodes():
            d[node.name] = node
        return d

    def remove_connection(self, node1, node2):
        self._graph[node1].remove(node2)

    def add_connection(self, node1, node2):
        """ Add connection between node1 and node2 """
        self.nodes.add(node1)
        self.nodes.add(node2)

        if node1 not in self._graph:
            self._graph[node1] = OrderedDict()
        self._graph[node1][node2] = True


    def remove(self, node):
        """ Remove all references to node """
        self.nodes.remove(node)
        for cxns in self._graph.keys():
            try:
                del cxns[node]
            except KeyError:
                pass
        try:
            del self._graph[node]
        except KeyError:
            pass

    def find_dependencies_top_down(self, start):

        if not start in self._graph.keys():
            return []

        queue = deque()
        queue.append(start)
        result = []
        while queue:
            node = queue.popleft()            
            if node in self._graph and node not in result:
                for n in self._graph[node].keys():
                    queue.append(n)
            result.append(node)
        return result[1:]
