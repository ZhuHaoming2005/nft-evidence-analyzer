//! Directed address graphs + Tarjan SCC (built once, reused).

use ahash::AHashMap;

use crate::enrich::{SaleEvent, TransferEvent};

/// Compact CSR digraph over addresses.
#[derive(Clone, Debug, Default)]
pub struct AddressGraph {
    pub addresses: Vec<String>,
    pub offsets: Vec<usize>,
    pub edges: Vec<usize>,
}

impl AddressGraph {
    pub fn from_transfers(transfers: &[TransferEvent]) -> Self {
        Self::build(transfers.iter().filter_map(|event| {
            if event.from.is_empty() || event.to.is_empty() || event.from == event.to {
                None
            } else {
                Some((event.from.as_str(), event.to.as_str()))
            }
        }))
    }

    pub fn from_sales(sales: &[SaleEvent]) -> Self {
        Self::build(sales.iter().filter_map(|event| {
            if event.seller.is_empty() || event.buyer.is_empty() || event.seller == event.buyer {
                None
            } else {
                Some((event.seller.as_str(), event.buyer.as_str()))
            }
        }))
    }

    pub fn from_sales_filtered(sales: &[SaleEvent], include: impl Fn(&SaleEvent) -> bool) -> Self {
        Self::build(
            sales
                .iter()
                .filter(|event| include(event))
                .filter_map(|event| {
                    if event.seller.is_empty()
                        || event.buyer.is_empty()
                        || event.seller == event.buyer
                    {
                        None
                    } else {
                        Some((event.seller.as_str(), event.buyer.as_str()))
                    }
                }),
        )
    }

    pub fn build<'a>(pairs: impl Iterator<Item = (&'a str, &'a str)>) -> Self {
        let mut unique = AHashMap::<&str, ()>::new();
        let mut collected = Vec::new();
        for (from, to) in pairs {
            unique.insert(from, ());
            unique.insert(to, ());
            collected.push((from, to));
        }
        let mut addresses = unique.into_keys().map(str::to_owned).collect::<Vec<_>>();
        addresses.sort();
        let by_address = addresses
            .iter()
            .enumerate()
            .map(|(index, address)| (address.as_str(), index))
            .collect::<AHashMap<_, _>>();
        let mut edge_pairs = collected
            .into_iter()
            .map(|(from, to)| (by_address[from], by_address[to]))
            .collect::<Vec<_>>();
        edge_pairs.sort_unstable();
        edge_pairs.dedup();
        let mut offsets = vec![0; addresses.len() + 1];
        for &(from, _) in &edge_pairs {
            offsets[from + 1] += 1;
        }
        for vertex in 0..addresses.len() {
            offsets[vertex + 1] += offsets[vertex];
        }
        let edges = edge_pairs.into_iter().map(|(_, to)| to).collect();
        Self {
            addresses,
            offsets,
            edges,
        }
    }

    pub fn strongly_connected_components(&self) -> Vec<Vec<usize>> {
        let mut state = Tarjan::new(self);
        for vertex in 0..self.addresses.len() {
            if state.indices[vertex].is_none() {
                state.visit_iterative(vertex);
            }
        }
        state.components
    }

    pub fn address_index(&self) -> AHashMap<&str, usize> {
        self.addresses
            .iter()
            .enumerate()
            .map(|(index, address)| (address.as_str(), index))
            .collect()
    }
}

struct Tarjan<'a> {
    graph: &'a AddressGraph,
    next_index: usize,
    indices: Vec<Option<usize>>,
    lowlink: Vec<usize>,
    stack: Vec<usize>,
    on_stack: Vec<bool>,
    components: Vec<Vec<usize>>,
}

impl<'a> Tarjan<'a> {
    fn new(graph: &'a AddressGraph) -> Self {
        let count = graph.addresses.len();
        Self {
            graph,
            next_index: 0,
            indices: vec![None; count],
            lowlink: vec![0; count],
            stack: Vec::new(),
            on_stack: vec![false; count],
            components: Vec::new(),
        }
    }

    fn discover(&mut self, vertex: usize) {
        let index = self.next_index;
        self.next_index += 1;
        self.indices[vertex] = Some(index);
        self.lowlink[vertex] = index;
        self.stack.push(vertex);
        self.on_stack[vertex] = true;
    }

    fn visit_iterative(&mut self, start: usize) {
        self.discover(start);
        let mut frames = vec![DfsFrame {
            vertex: start,
            next_edge: self.graph.offsets[start],
            parent: None,
        }];
        while let Some(frame) = frames.last_mut() {
            let vertex = frame.vertex;
            if frame.next_edge < self.graph.offsets[vertex + 1] {
                let next = self.graph.edges[frame.next_edge];
                frame.next_edge += 1;
                if self.indices[next].is_none() {
                    self.discover(next);
                    frames.push(DfsFrame {
                        vertex: next,
                        next_edge: self.graph.offsets[next],
                        parent: Some(vertex),
                    });
                } else if self.on_stack[next] {
                    self.lowlink[vertex] = self.lowlink[vertex]
                        .min(self.indices[next].expect("visited vertex has an index"));
                }
                continue;
            }

            let completed = frames.pop().expect("active DFS frame is present");
            if let Some(parent) = completed.parent {
                self.lowlink[parent] = self.lowlink[parent].min(self.lowlink[completed.vertex]);
            }
            let index = self.indices[completed.vertex].expect("discovered vertex has an index");
            if self.lowlink[completed.vertex] == index {
                let mut component = Vec::new();
                loop {
                    let member = self.stack.pop().expect("Tarjan stack is nonempty");
                    self.on_stack[member] = false;
                    component.push(member);
                    if member == completed.vertex {
                        break;
                    }
                }
                component.sort_unstable();
                self.components.push(component);
            }
        }
    }
}

struct DfsFrame {
    vertex: usize,
    next_edge: usize,
    parent: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enrich::SaleEvent;

    #[test]
    fn reciprocal_sales_form_one_scc() {
        let sales = vec![
            SaleEvent {
                tx_hash: "t0".into(),
                token_id: "1".into(),
                seller: "a".into(),
                buyer: "b".into(),
                timestamp: Some(1),
                block_number: Some(1),
                marketplace: None,
                native_amount: Some(1.0),
                usd_amount: Some(1.0),
                currency_symbol: None,
                currency_address: None,
                seller_proceeds_native: Some(1.0),
                seller_proceeds_usd: Some(1.0),
                ..SaleEvent::default()
            },
            SaleEvent {
                tx_hash: "t1".into(),
                token_id: "1".into(),
                seller: "b".into(),
                buyer: "a".into(),
                timestamp: Some(2),
                block_number: Some(2),
                marketplace: None,
                native_amount: Some(1.0),
                usd_amount: Some(1.0),
                currency_symbol: None,
                currency_address: None,
                seller_proceeds_native: Some(1.0),
                seller_proceeds_usd: Some(1.0),
                ..SaleEvent::default()
            },
        ];
        let graph = AddressGraph::from_sales(&sales);
        let components = graph.strongly_connected_components();
        assert_eq!(components.len(), 1);
        assert_eq!(components[0].len(), 2);
    }
}
