package com.netflix.lipstick.graph;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;

public class GraphTest {
    
    ObjectMapper mapper = new ObjectMapper();
    
    @Test
    public void testSerialization() {
        Map<String, Integer> counters = Maps.newHashMap();
        counters.put("num_records", 2);
        
        Graph graph = new Graph("0", "test")
            .property("userName", "testuser")
            .status(new Status(20, 1412354951l, 1412354796l, "running"))
            .node(new Node("a").property("alias","one").property("operation","start"))
            .node(new Node("b").property("alias", "two").property("operation","hop"))
            .node(new Node("c").property("alias", "three").property("operation","skip"))
            .node(new Node("d").property("alias", "four").property("operation","join"))
            .node(new Node("e").property("alias", "five").property("operation","read"))
            .node(new Node("f").property("alias", "six").property("operation","fly"))
            .node(new Node("g").property("alias", "seven").property("operation","dance"))
            .node(new Node("h").property("alias", "eight").property("operation","rollerskate"))
            .node(new Node("i").property("alias", "nine").property("operation","roll"))
            .node(new Node("j").property("alias", "ten").property("operation","dive"))
            .node(
                    new Node("k").type("Plain").url("http://www.netflix.com")
                    .property("alias", "eleven").property("operation", "tuck")
                    )
            .node(new Node("l").property("alias", "twelve").property("operation","pushup"))
            .node(new Node("m").property("alias", "thirteen").property("operation","twist"))
            .node(new Node("n").property("alias", "fourteen").property("operation","kick"))
            .node(new Node("o").property("alias", "fifteen").property("operation","shout"))
            .node(new Node("p").property("alias", "sixteen").property("operation","finish"))
            .node(new Node("job1").child("1"))
            .node(new Node("job2").child("2"))
            .node(new Node("job3").child("3"))
            .node(new Node("job4").child("4"))
            .node(new Node("job5").child("5"))
            .edge(new Edge("a","d"))
            .edge(new Edge("b","d"))
            .edge(new Edge("c","e").label("testlabel").type("Something").property("edgeWeight",2))
            .edge(new Edge("d","f"))
            .edge(new Edge("f","g"))
            .edge(new Edge("g","h"))
            .edge(new Edge("h","i"))
            .edge(new Edge("i","j"))
            .edge(new Edge("j","k"))
            .edge(new Edge("k","l"))
            .edge(new Edge("l","m"))
            .edge(new Edge("m","n"))
            .edge(new Edge("n","o"))
            .edge(new Edge("o","p"))
            .nodeGroup(
                    new NodeGroup("1")
                    .child("a").child("b")
                    .status(new Status(10, 1412354951l, 1412354796l, "failed"))
                    )
            .nodeGroup(new NodeGroup("2").child("c").child("d"))
            .nodeGroup(new NodeGroup("3").child("e").child("f").child("g"))
            .nodeGroup(new NodeGroup("4").child("h").child("i").child("j"))
            .nodeGroup(
                    new NodeGroup("5")
                    .children(Lists.newArrayList("k","l","m","n","o","p"))
                    .url("http://localhost:8080")
                    .property("counters", counters)
                    .status(
                            new Status()
                            .progress(30)
                            .startTime(1412354951l)
                            .heartbeatTime(1412354796l)
                            .statusText("running")
                            )
                    );
        try {
            String ser = CharStreams.toString(new InputStreamReader(GraphTest.class.getResourceAsStream("/graph.json")));
            JSONAssert.assertEquals(graph.toString(), ser, false);
        } catch (IOException e) {
            // err.
            e.printStackTrace();
        } catch (JSONException e) {
            // err.
            e.printStackTrace();
        }
    }
    
    @Test
    public void testDeserialization() {
        Graph graph = Graph.fromJson(GraphTest.class.getResourceAsStream("/graph.json"));
        
        Status expectedGraphStatus = new Status()
            .progress(20).startTime(1412354951l)
            .heartbeatTime(1412354796l).statusText("running");
        
        Map<String, Object> expectedGraphProperties = Maps.newHashMap();
        expectedGraphProperties.put("userName", "testuser");
        
        Assert.assertEquals(graph.id, "0");
        Assert.assertEquals(graph.name, "test");
        Assert.assertEquals(graph.status, expectedGraphStatus);
        Assert.assertEquals(graph.properties, expectedGraphProperties);
        
        // Look at a specificNode node in the graph
        Node expectedNode = new Node()
            .id("k").type("Plain").url("http://www.netflix.com")
            .property("alias", "eleven")
            .property("operation", "tuck");
        
        Assert.assertEquals(graph.node("k"), expectedNode);
        Assert.assertEquals(graph.node("job5").child, "5");
        
        // Look at a specific edge in the graph
        Edge expectedEdge = new Edge("c","e")
            .label("testlabel").type("Something")
            .property("edgeWeight", 2);
        Assert.assertEquals(graph.edge("c","e"), expectedEdge);
        
        // Look at a specific node group in the graph
        Map<String, Integer> counters = Maps.newHashMap();
        counters.put("num_records", 2);
        
        NodeGroup expectedGroup = new NodeGroup("5")
            .children(Lists.newArrayList("k","l","m","n","o","p"))
            .url("http://localhost:8080")
            .property("counters", counters)
            .status(
                    new Status()
                    .progress(30)
                    .startTime(1412354951l)
                    .heartbeatTime(1412354796l)
                    .statusText("running")
                    );    
        Assert.assertEquals(graph.nodeGroup("5"), expectedGroup);
        
        Assert.assertEquals(graph.numNodes(), 21);
        Assert.assertEquals(graph.numEdges(), 14);
        Assert.assertEquals(graph.numNodeGroups(), 5);
    }
}
