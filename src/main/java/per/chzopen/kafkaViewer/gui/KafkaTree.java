package per.chzopen.kafkaViewer.gui;

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import javax.swing.ImageIcon;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTree extends JTree
{

	private static final long serialVersionUID = 1L;

	public static Logger logger = LoggerFactory.getLogger(KafkaTree.class);
	
	public static ImageIcon IMAGEICON_QUESTION = new ImageIcon(ClassLoader.getSystemClassLoader().getResource("images/question.png"));

	private KafkaTree _this = this;
	
	private JPopupMenu popupMenu;
	private JMenuItem menuItemRefresh;
	private JMenuItem menuItemView;
	private JMenuItem menuItemOpen;
	
	private KafkaTreeNode root;
	private KafkaTreeNode brokersNode;
	private KafkaTreeNode topicsNode;
	
	private CuratorFramework curatorClient;
	
	private ListenerHelper listenerHelper = new ListenerHelper();

	private TreePath selectedTreePath;
	private KafkaTreeNode selectedTreeNode;
	
	public KafkaTree()
	{
		this.setCellRenderer(new MyTreeCellRenderer());
		
		// 右键菜单相关
		{
			popupMenu = new JPopupMenu();
			menuItemRefresh = new JMenuItem("刷新");
			menuItemView = new JMenuItem("查看");
			menuItemOpen = new JMenuItem("打开");
		}

		initEvent();
	}

	public void setMessage(String message)
	{
		clear();
		
		root = new KafkaTreeNode(message, NodeType.root);
		this.setModel(new DefaultTreeModel(root));
		this.setRootVisible(true);
	}
	
	public void setZookeeper(CuratorFramework curatorClient)
	{
		clear();
		this.curatorClient = curatorClient;

		root = new KafkaTreeNode("/", NodeType.root);
		
		brokersNode = new KafkaTreeNode("brokers", NodeType.brokers);
		root.insert(brokersNode, root.getChildCount());	

		topicsNode = new KafkaTreeNode("topics", NodeType.topics);
		root.insert(topicsNode, root.getChildCount());

		this.setModel(new DefaultTreeModel(root));
		this.setRootVisible(false);
		this.expandPath(new TreePath(root));
	}
	
	private void clear()
	{
		if( curatorClient!=null )
		{
			curatorClient.close();
		}
	}

	public void addListener(Listener listener)
	{
		listenerHelper.addListener(listener);
	}
	
	private void showPopup(KafkaTreeNode treeNode, int x, int y)
	{
		if( treeNode.getType()==NodeType.brokers )
		{
			popupMenu.removeAll();
			popupMenu.add(menuItemRefresh);
		}
		else if( treeNode.getType()==NodeType.broker )
		{
			popupMenu.removeAll();
			popupMenu.add(menuItemView);
		}
		else if( treeNode.getType()==NodeType.topics )
		{
			popupMenu.removeAll();
			popupMenu.add(menuItemRefresh);
		}
		else if( treeNode.getType()==NodeType.topic )
		{
			popupMenu.removeAll();
			popupMenu.add(menuItemRefresh);
			popupMenu.addSeparator();
			popupMenu.add(menuItemView);
			popupMenu.add(menuItemOpen);
		}
		else if( treeNode.getType()==NodeType.partitions )
		{
			popupMenu.removeAll();
			popupMenu.add(menuItemRefresh);
		}
		else if( treeNode.getType()==NodeType.partition )
		{
			popupMenu.removeAll();
			popupMenu.add(menuItemView);
		}
		popupMenu.show(_this, x, y);
		_this.getSelectionModel().setSelectionPath(selectedTreePath);
		_this.grabFocus();
	}
	
	public void initEvent()
	{
		// 鼠标事件
		this.addMouseListener(new MouseAdapter()
		{
			@Override
			public void mouseReleased(MouseEvent event)
			{
				// 右键单击
				if( event.getClickCount()==1 && event.getButton()==MouseEvent.BUTTON3 )
				{
					selectedTreePath = _this.getPathForLocation(event.getX(), event.getY());
					if( selectedTreePath==null )
					{
						return ;
					}
					selectedTreeNode = (KafkaTreeNode)selectedTreePath.getLastPathComponent();
					
					showPopup(selectedTreeNode, event.getX(), event.getY());
					popupMenu.show(_this, event.getX(), event.getY());
				}
			}
			
			public void mouseClicked(MouseEvent event)
			{
				try
				{
					selectedTreePath = _this.getPathForLocation(event.getX(), event.getY());
					if( selectedTreePath==null )
					{
						return ;
					}
					selectedTreeNode = (KafkaTreeNode)selectedTreePath.getLastPathComponent();
					if( event.getClickCount()==1 )
					{
						// 左键单击
						if( event.getButton()==MouseEvent.BUTTON1 )
						{
//							Stat stat = new Stat();
//							byte[] data = curatorClient.getData().storingStatIn(stat).forPath(selectedTreeNode.getKafkaPath());
//							listenerHelper.fireOnNodeClicked(event, selectedTreeNode, stat, data);
						}
						
					}
					// 双击
					else if (event.getClickCount() == 2)
					{
						if (selectedTreeNode.isLoaded() == false)
						{
							refreshTreeNode(selectedTreeNode, selectedTreePath);
							_this.expandPath(selectedTreePath);
							_this.updateUI();
						}
					}
				}
				catch (Exception e)
				{
					logger.error("", e);
				}
			}
		});
		
		// 刷新
		menuItemRefresh.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				try
				{
					refreshTreeNode(selectedTreeNode, selectedTreePath);
					_this.expandPath(selectedTreePath);
					_this.updateUI();
				}
				catch (Exception e1)
				{
					logger.error("", e);
				}
			}
		});
		
		// 查看
		menuItemView.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				try
				{
					KafkaTreeNode treeNode = selectedTreeNode;
					
					if( treeNode.getType()==NodeType.broker )
					{
						byte[] data = curatorClient.getData().forPath("/brokers/ids/"+treeNode.getName());
						listenerHelper.fireOnBrokerData(treeNode, data);
					}
					else if( treeNode.getType()==NodeType.topic )
					{
						byte[] data = curatorClient.getData().forPath(String.format("/brokers/topics/%s", treeNode.getName()));
						listenerHelper.fireOnTopicData(treeNode, data);
					}
					else if( treeNode.getType()==NodeType.partition )
					{
						KafkaTreeNode topicNode = (KafkaTreeNode)treeNode.getParent().getParent();
						byte[] data = curatorClient.getData().forPath(String.format("/brokers/topics/%s/partitions/%s/state", topicNode.getName(), treeNode.getName()));
						listenerHelper.fireOnPartitionData(treeNode, data);
					}
				}
				catch (Exception e1)
				{
					logger.error("", e);
				}
			}
		});
		
		// 打开
		menuItemOpen.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				try
				{
					KafkaTreeNode treeNode = selectedTreeNode;
					
					if( treeNode.getType()==NodeType.topic )
					{
						listenerHelper.fireOnTopicOpen(treeNode, treeNode.getName());
					}
				}
				catch (Exception e1)
				{
					logger.error("", e);
				}
			}
		});
	}
	
	
	private void refreshTreeNode(KafkaTreeNode treeNode, TreePath treePath) throws Exception
	{
		if( treeNode.getType()==NodeType.brokers )
		{
			treeNode.setLoaded(true);
			treeNode.removeAllChildren();
			List<String> children = curatorClient.getChildren().forPath("/brokers/ids");
			for (String childName : children)
			{
				KafkaTreeNode childNode = new KafkaTreeNode(childName, NodeType.broker);
				childNode.setLoaded(true);
				treeNode.insert(childNode, treeNode.getChildCount());
			}
		}
		else if( treeNode.getType()==NodeType.topics )
		{
			treeNode.setLoaded(true);
			treeNode.removeAllChildren();
			List<String> children = curatorClient.getChildren().forPath("/brokers/topics");
			List<KafkaTreeNode> tmpList = new ArrayList<>();
			for (String childName : children)
			{
				tmpList.add(new KafkaTreeNode(childName, NodeType.topic));
			}
			tmpList.sort((KafkaTreeNode o1, KafkaTreeNode o2) -> o1.compareAsString(o2));
			addChildren(treeNode, tmpList, false);
		}
		else if( treeNode.getType()==NodeType.topic )
		{
			treeNode.setLoaded(true);
			treeNode.removeAllChildren();
			KafkaTreeNode childNode = new KafkaTreeNode("partitions", NodeType.partitions);
			treeNode.insert(childNode, treeNode.getChildCount());
		}
		else if( treeNode.getType()==NodeType.partitions )
		{
			treeNode.setLoaded(true);
			treeNode.removeAllChildren();
			treePath.getParentPath();
			KafkaTreeNode paNode = (KafkaTreeNode)treePath.getParentPath().getLastPathComponent();
			List<String> children = curatorClient.getChildren().forPath(String.format("/brokers/topics/%s/partitions", paNode.getName()));
			List<KafkaTreeNode> tmpList = new ArrayList<>();
			for (String childName : children)
			{
				tmpList.add(new KafkaTreeNode(childName, NodeType.partition));
			}
			tmpList.sort((KafkaTreeNode o1, KafkaTreeNode o2) -> o1.compareAsDecimal(o2));
			addChildren(treeNode, tmpList, true);
		}
	}
	
	private void addChildren(KafkaTreeNode paNode, List<KafkaTreeNode> childrenNode, boolean loaded)
	{
		for( KafkaTreeNode childNode : childrenNode )
		{
			childNode.setLoaded(loaded);
			paNode.insert(childNode, paNode.getChildCount());
		}
	}
	
	
	/**
	 * 
	 */
	public static class KafkaTreeNode extends DefaultMutableTreeNode
	{
		private static final long serialVersionUID = 1L;

		private boolean loaded = false;
		private NodeType type = NodeType.none;

		public KafkaTreeNode(String name, NodeType nodeType)
		{
			super(name==null ? "" : name);
			this.type = nodeType;
		}
		
		public String getName()
		{
			return (String)this.getUserObject();
		}

		public boolean isLoaded()
		{
			return loaded;
		}

		public void setLoaded(boolean loaded)
		{
			this.loaded = loaded;
		}
		
		public NodeType getType()
		{
			return this.type;
		}

		public int compareAsString(KafkaTreeNode o2)
		{
			return this.getName().compareTo(o2.getName());
		}
		public int compareAsDecimal(KafkaTreeNode o2)
		{
			BigDecimal d1 = new BigDecimal(this.getName());
			BigDecimal d2 = new BigDecimal(o2.getName());
			return d1.compareTo(d2);
		}
	}

	/**
	 * 
	 */
	public class MyTreeCellRenderer extends DefaultTreeCellRenderer
	{
		private static final long serialVersionUID = 1L;

		@Override
		public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row, boolean hasFocus)
		{
			super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);
			if( value instanceof KafkaTreeNode )
			{
				KafkaTreeNode treeNode = (KafkaTreeNode)value;
				if( treeNode.getType()==NodeType.none || treeNode.getType()==NodeType.root )
				{
					return this;
				}
				if( !treeNode.isLoaded() )
				{
					setLeafIconByValue(treeNode);
				}
			}
			return this;
		}

		public void setLeafIconByValue(KafkaTreeNode treeNode)
		{
			this.setIcon(IMAGEICON_QUESTION);
		}
	}

	/**
	 * 
	 */
	public static enum NodeType
	{
		none,
		root,
		brokers,
		broker,
		topics,
		topic,
		partitions,
		partition
	}

	/**
	 * 
	 */
	public static interface Listener
	{
		public void onBrokerData(KafkaTreeNode treeNode, byte[] data) throws Exception;
		public void onTopicData(KafkaTreeNode treeNode, byte[] data) throws Exception;
		public void onTopicOpen(KafkaTreeNode treeNode, String topic) throws Exception;
		public void onPartitionData(KafkaTreeNode treeNode, byte[] data) throws Exception;
	}
	
	/**
	 * 
	 */
	public static class ListenerHelper
	{
		public LinkedHashMap<Listener, Object> listeners = new LinkedHashMap<>();
		
		public void addListener(Listener listener)
		{
			listeners.put(listener, listener);
		}
		
		public void fireOnBrokerData(KafkaTreeNode treeNode, byte[] data)
		{
			for( Entry<KafkaTree.Listener,Object> entry: listeners.entrySet() )
			{
				try
				{
					entry.getKey().onBrokerData(treeNode, data);
				}
				catch (Exception e)
				{
					logger.error("", e);
				}
			}
		}

		public void fireOnTopicData(KafkaTreeNode treeNode, byte[] data)
		{
			for( Entry<KafkaTree.Listener,Object> entry: listeners.entrySet() )
			{
				try
				{
					entry.getKey().onTopicData(treeNode, data);
				}
				catch (Exception e)
				{
					logger.error("", e);
				}
			}
		}

		public void fireOnTopicOpen(KafkaTreeNode treeNode, String topic)
		{
			for( Entry<KafkaTree.Listener,Object> entry: listeners.entrySet() )
			{
				try
				{
					entry.getKey().onTopicOpen(treeNode, topic);
				}
				catch (Exception e)
				{
					logger.error("", e);
				}
			}
		}

		public void fireOnPartitionData(KafkaTreeNode treeNode, byte[] data)
		{
			for( Entry<KafkaTree.Listener,Object> entry: listeners.entrySet() )
			{
				try
				{
					entry.getKey().onPartitionData(treeNode, data);
				}
				catch (Exception e)
				{
					logger.error("", e);
				}
			}
		}
	}
}
