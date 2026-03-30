<script lang="ts">
  import Router from "svelte-spa-router";
  import Layout from "./components/Layout.svelte";
  import Dashboard from "./pages/Dashboard.svelte";
  import Topics from "./pages/Topics.svelte";
  import TopicDetail from "./pages/TopicDetail.svelte";
  import Nodes from "./pages/Nodes.svelte";
  import NodeDetail from "./pages/NodeDetail.svelte";
  import ConsumerGroups from "./pages/ConsumerGroups.svelte";
  import GroupDetail from "./pages/GroupDetail.svelte";
  import EventInspector from "./pages/EventInspector.svelte";
  import Overrides from "./pages/Overrides.svelte";
  import DLQ from "./pages/DLQ.svelte";

  const routes = {
    "/": Dashboard,
    "/topics": Topics,
    "/topics/:name": TopicDetail,
    "/nodes": Nodes,
    "/nodes/:id": NodeDetail,
    "/consumer-groups": ConsumerGroups,
    "/consumer-groups/:id": GroupDetail,
    "/events": EventInspector,
    "/overrides": Overrides,
    "/dlq": DLQ,
  };
</script>

<Layout>
  <Router {routes} />
</Layout>
