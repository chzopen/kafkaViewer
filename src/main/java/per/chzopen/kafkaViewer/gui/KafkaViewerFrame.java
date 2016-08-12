package per.chzopen.kafkaViewer.gui;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.concurrent.TimeUnit;

import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JTabbedPane;
import javax.swing.SwingUtilities;
import javax.swing.border.BevelBorder;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import per.chzopen.kafkaViewer.common.define.Releasable;
import per.chzopen.kafkaViewer.gui.KafkaTree.KafkaTreeNode;

public class KafkaViewerFrame extends JFrame
{

	private static final long serialVersionUID = 1L;
	
	private static Logger logger = LoggerFactory.getLogger(KafkaViewerFrame.class);

	private KafkaViewerFrame _this = this;
	
	private JMenuItem mItemSwitchKafka;
	private JMenuItem mItemAbout;

	private TabbedPanePopup tabbedPanePopup;
	
	private JTabbedPane tabbedPane;
	private IndexTabPanel indexTabPanel;
	
	private JLabel statusLabel;
	
	
	private String zkAddr = "localhost:2181";
	private CuratorFramework curatorClient;
	
	
	public KafkaViewerFrame()
	{
		this.setTitle("kafkaViewer");
		initMenu();
		initPopupMenu();
		initTabbedPane();
		initStatusBar();
		initEvent();
	}
	
	private void initPopupMenu()
	{
		tabbedPanePopup = new TabbedPanePopup();
	}
	
	private void initTabbedPane()
	{
		tabbedPane = new JTabbedPane();
		
		indexTabPanel = new IndexTabPanel();
		tabbedPane.insertTab("Index", null, indexTabPanel, "", 0);
		
		this.add(tabbedPane);
	}
	
	private void initMenu()
	{
		JMenuBar menuBar = new JMenuBar();
		
		// Settings
		{
			JMenu menuSettings = new JMenu("Settings");
			mItemSwitchKafka = new JMenuItem("Switch kafka");
			menuSettings.add(mItemSwitchKafka);	
			menuBar.add(menuSettings);
		}
		
		// Help
		{
			JMenu menuHelp = new JMenu("Help");
			mItemAbout = new JMenuItem("About");
			menuHelp.add(mItemAbout);	
			menuBar.add(menuHelp);
		}
		
		this.setJMenuBar(menuBar);
	}
	

	private void initStatusBar()
	{
		JPanel statusBar = new JPanel();
		statusBar.setBorder(new BevelBorder(BevelBorder.LOWERED));
		statusBar.setPreferredSize(new Dimension(30, 30));
		statusBar.setLayout(new BoxLayout(statusBar, BoxLayout.X_AXIS));

		statusLabel = new JLabel("Welcome using kafkaViewer");
		statusBar.add(statusLabel);
		
		this.add(statusBar, BorderLayout.SOUTH);
	}
	
	private void initEvent()
	{
		// Switch kafka
		mItemSwitchKafka.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				Object rt = JOptionPane.showInputDialog(_this, "Please input zookeeper address:", "Input", JOptionPane.QUESTION_MESSAGE, null, null, zkAddr);
				if( rt==null )
				{
					return ;
				}
				String str = ""+rt;
				if( StringUtils.isBlank(str) )
				{
					JOptionPane.showMessageDialog(null, "Input content cannot be empty");
					return ;
				}
				
				// 关闭其它的窗口
				for( Component component : tabbedPane.getComponents())
				{
					if( component!=indexTabPanel )
					{
						tabbedPane.remove(component);
					}
				}
				
				
				String message = String.format("connecting to '%s'", str);
				statusLabel.setText(message);
				indexTabPanel.getKafkaTree().setMessage(message);
				indexTabPanel.getKafkaTree().updateUI();
				zkAddr = str;
				SwingUtilities.invokeLater(new Runnable()
				{
					public void run()
					{
						try
						{
							RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
							if( curatorClient!=null )
							{
								curatorClient.close();
							}
							curatorClient = CuratorFrameworkFactory.newClient(zkAddr, retryPolicy);
							curatorClient.start();
							curatorClient.blockUntilConnected(2, TimeUnit.SECONDS);
							if( curatorClient.getZookeeperClient().isConnected() )
							{
								String message = String.format("connect to zookeeper '%s' succeeded", zkAddr);
								statusLabel.setText(message);
								indexTabPanel.getKafkaTree().setZookeeper(curatorClient);
							}
							else
							{
								curatorClient.close();
								String message = String.format("connect to zookeeper '%s' failed", zkAddr);
								statusLabel.setText(message);
								indexTabPanel.getKafkaTree().setMessage(message);
								indexTabPanel.getKafkaTree().updateUI();
							}
						}
						catch (Exception e)
						{
							String message = String.format("connect to zookeeper '%s' failed", zkAddr);
							statusLabel.setText(message);
							indexTabPanel.getKafkaTree().setMessage(message);
							indexTabPanel.getKafkaTree().updateUI();
						}
					}
				});
			}
		});
		
		// About
		mItemAbout.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				StringBuilder sb = new StringBuilder();
				sb.append("Authored by Chzopen.\r\n");
				sb.append("Contact author by mailing to chzopen@163.com");
				JOptionPane.showMessageDialog(_this, sb.toString());				
			}
		});

		// tab显示右键菜单
		tabbedPane.addMouseListener(new MouseAdapter()
		{
			public void mouseReleased(MouseEvent event)
			{
				if( event.getClickCount()==1 && event.getButton()==MouseEvent.BUTTON3 )
				{
					int tabIndex = tabbedPane.indexAtLocation(event.getX(), event.getY());
					if( tabIndex>=0 )
					{
						Component c = event.getComponent();
						tabbedPanePopup.popup.show(c, event.getX(), event.getY());
					}
				}
			}
		});
		
		// 关闭tab
		tabbedPanePopup.mItemClose.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent event)
			{
				try
				{
					Component component = tabbedPane.getComponentAt(tabbedPane.getSelectedIndex());
					if( component==indexTabPanel )
					{
						JOptionPane.showMessageDialog(_this, "Index页面不能关闭");
					}
					else
					{
						if( component instanceof Releasable )
						{
							((Releasable) component).release();
						}
						tabbedPane.remove(component);
					}
				}
				catch (Exception e)
				{
					logger.error("", e);
				}
			}
		});
		
		// 打开topic
		indexTabPanel.getKafkaTree().addListener(new KafkaTree.Listener()
		{
			public void onTopicOpen(KafkaTreeNode treeNode, String topic) throws Exception
			{
				if( !_this.curatorClient.getZookeeperClient().isConnected() )
				{
					JOptionPane.showMessageDialog(_this, "Zookeeper isn't connected");
					return ;
				}
				
				String title = "topic:" + topic;
				int tabCount = tabbedPane.getTabCount();
				for( int i=0; i<tabCount; i++ )
				{
					if( title.equals(tabbedPane.getTitleAt(i)) )
					{
						tabbedPane.setSelectedIndex(i);
						return ;
					}
				}
				TopicTabPanel topicTabPanel = new TopicTabPanel(topic);
				topicTabPanel.setCuratorClient(_this.curatorClient);
				tabbedPane.addTab(title, topicTabPanel);
				tabbedPane.setSelectedComponent(topicTabPanel);
				topicTabPanel.initConsumer();
				topicTabPanel.fetchData();
			}
			
			public void onTopicData(KafkaTreeNode treeNode, byte[] data) throws Exception
			{
			}
			
			public void onPartitionData(KafkaTreeNode treeNode, byte[] data) throws Exception
			{
			}
			
			public void onBrokerData(KafkaTreeNode treeNode, byte[] data) throws Exception
			{
			}
		});
	}
	
	/**
	 * 
	 */
	public JLabel getStatusLabel()
	{
		return statusLabel;
	}
	
	/**
	 * 
	 */
	public static class TabbedPanePopup
	{
		public JPopupMenu popup;
		
		public JMenuItem mItemClose;
		
		public TabbedPanePopup()
		{
			popup = new JPopupMenu();
			mItemClose = new JMenuItem("关闭");
			popup.add(mItemClose);
		}
	}
}




