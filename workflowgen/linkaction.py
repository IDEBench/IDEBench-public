import random
from collections import OrderedDict
from common.operation import Operation
from workflowgen.baseaction import BaseAction
from workflowgen.vizaction import VizAction

class LinkAction(BaseAction):

    def get_states(self):

        if VizAction.VIZ_COUNTER < 1:
            return None

        pick_from = -1
        pick_to = -1
        link_type = None
        while (pick_from == -1 or pick_to == -1):
            from_candidate = random.randint(0, VizAction.VIZ_COUNTER)
            to_candidate = random.randint(0, VizAction.VIZ_COUNTER)

            
            link_type_names = [l["name"] for l in self.config["linkType"]]
            link_type_pds = [l["p"] for l in self.config["linkType"]]

            link_type = self.pick(link_type_names, link_type_pds)
            print(link_type)
           
            if link_type == "sequential" and LinkAction.LATEST_LINK:
                from_candidate = LinkAction.LATEST_LINK[1]
            elif link_type == "1n" and LinkAction.LATEST_LINK:
                from_candidate = LinkAction.LATEST_LINK[0]
            elif link_type == "n1" and LinkAction.FIRST_LINK:
                to_candidate = LinkAction.FIRST_LINK[1]

            num_tries = 10
            giveup = False
            g = {}
            for i in range(num_tries+1):
                
                g = {}
                for l in LinkAction.LINKS:
                    if l[0] not in g:
                        g[l[0]] = []
                    g[l[0]].append(l[1])

                if from_candidate not in g:
                    g[from_candidate] = []
                g[from_candidate].append(to_candidate)

                if self.cyclic(g):
                    if link_type == "n1" and LinkAction.FIRST_LINK:
                        to_candidate = LinkAction.FIRST_LINK[1]
                    else:
                        to_candidate = random.randint(0, VizAction.VIZ_COUNTER)
                    if i == num_tries:
                       
                        giveup = True
                else:
                    break

            if giveup:
                print("giving up!")
                break

            if from_candidate != to_candidate and  ((to_candidate, from_candidate) not in LinkAction.LINKS) and ((from_candidate, to_candidate) not in LinkAction.LINKS):

                pick_from = from_candidate
                pick_to = to_candidate
                
                if not LinkAction.FIRST_LINK:
                    LinkAction.FIRST_LINK = (pick_from, pick_to)
                LinkAction.LATEST_LINK = (pick_from, pick_to)
                LinkAction.LINKS.add(LinkAction.LATEST_LINK)
                break

            if len(LinkAction.LINKS) >= VizAction.VIZ_COUNTER-1:
                break
        #print("a")
        if (pick_from == -1 or pick_to == -1):
        #    print("b")
            return None
        #print("c")
    
        incoming_links = [ "viz_" + str(l[0]) for l in filter(lambda x: x[1] == pick_to, LinkAction.LINKS)]
        combined_filters = Operation(OrderedDict({"name": "viz_" + str(pick_to), "source": ( " and ".join(incoming_links))}))
        return combined_filters

    def cyclic(self, g):
        path = set()
        def visit(vertex):
            path.add(vertex)
            for neighbour in g.get(vertex, ()):
                if neighbour in path or visit(neighbour):
                    return True
            path.remove(vertex)
            return False

        return any(visit(v) for v in g)

LinkAction.FIRST_LINK = None
LinkAction.LATEST_LINK = None
LinkAction.LINKS = set()
